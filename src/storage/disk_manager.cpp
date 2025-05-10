/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

DiskManager::DiskManager() {
    memset(fd2pageno_, 0,
           MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char)));
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset,
                             int num_bytes) {
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    off_t offset_in_file = lseek(fd, page_no * PAGE_SIZE, SEEK_SET);
    if (offset_in_file == -1) {
        throw InternalError("DiskManager::write_page Error: lseek failed");
    }

    // 2.调用write()函数
    ssize_t bytes_written = write(fd, offset, num_bytes);

    // 注意write返回值与num_bytes不等时 throw
    // InternalError("DiskManager::write_page Error");
    if (bytes_written != num_bytes) {
        throw InternalError("DiskManager::write_page Error: write failed");
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset,
                            int num_bytes) {
    // 1. 使用 lseek() 函数将文件指针定位到指定页面的起始位置
    //    通过 (fd, page_no) 可以计算出页面在文件中的偏移量
    off_t offset_in_file = lseek(fd, page_no * PAGE_SIZE, SEEK_SET);
    if (offset_in_file == -1) {
        // 如果 lseek 调用失败，抛出 InternalError 异常
        throw InternalError("DiskManager::read_page Error: lseek failed");
    }

    // 2. 使用 read() 函数从文件中读取指定数量的字节到内存中
    //    读取的数据将被存储在 offset 指向的缓冲区中
    ssize_t bytes_read = read(fd, offset, num_bytes);

    // 注意：如果 read() 函数返回的字节数与 num_bytes 不相等，说明读取操作失败
    //    抛出 InternalError 异常
    if (bytes_read != num_bytes) {
        throw InternalError("DiskManager::read_page Error: read failed");
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    // 首先检查文件是否已经存在，避免重复创建
    if (is_file(path)) {
        throw FileExistsError(path);  // 文件存在时抛出异常
    }

    // 使用open()函数创建文件，O_CREAT标志表示如果文件不存在则创建
    // O_RDWR标志表示打开文件用于读写操作
    // S_IRUSR | S_IWUSR 模式表示文件所有者具有读写权限
    int fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

    // 检查open()函数的返回值，小于0表示创建失败，抛出异常
    if (fd < 0) {
        throw UnixError();
    }

    // 关闭文件描述符，因为我们只是创建文件，无需对其进行进一步操作
    close(fd);
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // 首先检查文件是否存在
    if (!is_file(path)) {
        throw FileNotFoundError(path);  // 如果文件不存在，抛出异常
    }

    // 检查文件是否已经被关闭
    if (path2fd_.find(path) != path2fd_.end()) {
        throw FileNotClosedError(path);  // 如果文件未关闭，抛出异常
    }

    // 调用unlink()函数删除文件
    // 注意不能删除未关闭的文件
    if (unlink(path.c_str()) < 0) {
        throw UnixError();  // 如果unlink()函数返回值小于0，表示删除失败，抛出异常
    }
}

/**
 * @description: 打开指定路径文件
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    // 首先检查文件是否已经存在，避免尝试打开不存在的文件
    if (!is_file(path)) {
        throw FileNotFoundError(path);  // 如果文件不存在，抛出异常
    }

    // 确保文件没有被重复打开，每个文件只能有一个文件描述符
    if (path2fd_.find(path) != path2fd_.end()) {
        throw FileNotClosedError(path);  // 如果文件已经被打开，抛出异常
    }

    // 使用open()函数打开文件
    // O_RDWR标志表示文件可以进行读写操作
    int fd = open(path.c_str(), O_RDWR);

    // 检查open()函数的返回值
    // 如果返回值小于0，表示打开文件失败，抛出异常
    if (fd < 0) {
        throw UnixError();  // 打开文件失败，抛出异常
    }

    // 将文件路径与打开的文件描述符添加到映射中
    // 这是为了后续操作可以通过文件路径找到文件描述符
    path2fd_[path] = fd;

    // 将文件描述符与文件路径添加到另一个映射中
    // 这是为了后续操作可以通过文件描述符找到文件路径
    fd2path_[fd] = path;

    // 返回打开的文件描述符
    return fd;
}

/**
 * @description: 用于关闭指定路径文件
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // 首先检查文件描述符是否在映射中存在
    if (fd2path_.find(fd) == fd2path_.end()) {
        throw FileNotOpenError(
            fd);  // 如果文件描述符不在映射中，表示文件未打开，抛出异常
    }

    // 调用close()函数来关闭文件
    // 注意不能关闭未打开的文件
    if (close(fd) < 0) {
        throw UnixError();  // 如果close()函数返回值小于0，表示关闭文件失败，抛出异常
    }

    // 从path2fd_映射中移除该文件的路径
    // 这是为了更新文件打开列表，确保文件已经关闭
    path2fd_.erase(fd2path_[fd]);

    // 从fd2path_映射中移除该文件的描述符
    // 这是为了更新文件打开列表，确保文件已经关闭
    fd2path_.erase(fd);
}


/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if (size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}