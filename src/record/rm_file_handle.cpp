/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL
v2. You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

#include "common/context.h"
#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid,
                                                   Context* context) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    /*调用 fetch_page_handle 从缓冲池中获取指定页面，并封装为 RmPageHandle 对象
    （包含页面头、位图、槽位数组等元信息）。*/
    char* slot = page_handle.get_slot(rid.slot_no);
    /*通过 get_slot 计算目标槽位的物理地址：
      槽位地址 = slots起始地址 + slot_no * record_size */
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    memcpy(record->data, slot, file_hdr_.record_size);
    // 根据文件头中定义的 record_size 分配内存，并将槽位数据复制到新创建的
    // RmRecord 中。
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    // 操作完成后解除页面锁定（pin_count--），false
    // 表示不强制刷盘（因未修改页面）。
    return record;
}

/**
 * @description: 插入一条新记录到文件中
 * @param {char*} buf 要插入的记录数据缓冲区
 * @param {Context*} context 事务上下文（本实验未使用）
 * @return {Rid} 返回新插入记录的ID（页面号+槽位号）
 *
 * 实现步骤：
 * 1. 获取一个有空闲槽位的页面（优先复用空闲页，无则创建新页）
 * 2. 在页面中查找第一个空闲槽位
 * 3. 更新位图标记槽位已使用
 * 4. 将记录数据复制到槽位中
 * 5. 更新页面记录计数
 * 6. 如果页面变满，更新空闲页链表
 * 7. 返回新记录的RID
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 步骤1：获取可用页面（自动处理空闲页或创建新页）
    RmPageHandle page_handle = create_page_handle();

    // 步骤2：查找页面中第一个空闲槽位
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap,
                                    file_hdr_.num_records_per_page);

    // 步骤3：设置位图标记槽位已使用
    Bitmap::set(page_handle.bitmap, slot_no);

    // 步骤4：将记录数据复制到槽位
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);

    // 步骤5：增加页面记录计数
    page_handle.page_hdr->num_records++;

    // 步骤6：检查页面是否变满
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 从空闲链表移除该页：原next指针成为新的链表头
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        // 当前页标记为无后续空闲页
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    }

    // 构造新记录的RID（页面号+槽位号）
    Rid rid{page_handle.page->get_page_id().page_no, slot_no};

    // 解除页面锁定（dirty=true因为修改了页面内容）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);

    return rid;
}

/**
 * @description: 删除指定记录
 * @param {Rid&} rid 要删除记录的ID
 * @param {Context*} context 事务上下文（本实验未使用）
 *
 * 实现步骤：
 * 1. 获取记录所在页面
 * 2. 检查删除前页面是否已满（用于判断是否需要加入空闲链表）
 * 3. 重置位图中对应位
 * 4. 减少页面记录计数
 * 5. 如果页面从满变为不满，将其加入空闲链表
 * 6. 解除页面锁定
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 步骤1：获取记录所在页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 步骤2：记录删除前页面是否已满
    bool was_full =
        (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);

    // 步骤3：重置位图标记槽位为空闲
    Bitmap::reset(page_handle.bitmap, rid.slot_no);

    // 步骤4：减少页面记录计数
    page_handle.page_hdr->num_records--;

    // 步骤5：如果页面从满变为不满，加入空闲链表
    if (was_full) {
        release_page_handle(page_handle);
    }

    // 步骤6：解除页面锁定（dirty=true因为修改了页面内容）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新指定记录的内容
 * @param {Rid&} rid 要更新记录的ID
 * @param {char*} buf 新记录数据缓冲区
 * @param {Context*} context 事务上下文（本实验未使用）
 *
 * 实现步骤：
 * 1. 获取记录所在页面
 * 2. 直接将新数据覆盖到原槽位
 * 3. 解除页面锁定
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 步骤1：获取记录所在页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);

    // 步骤2：覆盖槽位数据（不需要修改位图或记录计数）
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);

    // 步骤3：解除页面锁定（dirty=true因为修改了页面内容）
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 获取指定页面的句柄
 * @param {int} page_no 要获取的页面号
 * @return {RmPageHandle} 页面句柄对象
 * @throws {PageNotExistError} 当页面不存在时抛出异常
 *
 * 实现步骤：
 * 1. 检查页面号有效性
 * 2. 从缓冲池获取页面
 * 3. 构造并返回页面句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // 步骤1：检查页面号范围
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("", page_no);
    }

    // 构造页面ID（文件描述符+页面号）
    PageId page_id = {.fd = fd_, .page_no = page_no};

    // 步骤2：从缓冲池获取页面
    Page* page = buffer_pool_manager_->fetch_page(page_id);
    if (!page) {
        throw PageNotExistError("Failed to fetch page", page_no);
    }

    // 步骤3：构造页面句柄（自动解析页面头、位图和槽位）
    return RmPageHandle(&file_hdr_, page);
}

RmPageHandle RmFileHandle::create_new_page_handle() {
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    Page* new_page = buffer_pool_manager_->new_page(&new_page_id);
    if (!new_page) {
        throw InternalError("No free pages available");
    }

    // 初始化页面头
    RmPageHdr page_hdr{};
    page_hdr.next_free_page_no = -1;
    page_hdr.num_records = 0;
    memcpy(new_page->get_data(), &page_hdr, sizeof(RmPageHdr));

    // 初始化位图为全0
    char* bitmap = new_page->get_data() + sizeof(RmPageHdr);
    Bitmap::init(bitmap, file_hdr_.bitmap_size);

    // 更新文件头信息
    file_hdr_.num_pages++;
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_,
                              sizeof(file_hdr_));

    return RmPageHandle(&file_hdr_, new_page);
}

/**
 * @description: 创建或获取一个空闲页面句柄
 * @return {RmPageHandle} 返回可用的页面句柄
 *
 * 实现逻辑：
 * 1. 如果没有空闲页(first_free_page_no == RM_NO_PAGE)，则创建新页
 * 2. 否则获取第一个空闲页，并更新空闲页链表头指针
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 情况1：当前没有空闲页可用
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 创建全新的页面（会初始化页面头、位图，并更新文件头）
        return create_new_page_handle();
    }

    // 情况2：有空闲页可用
    // 获取当前第一个空闲页的句柄（从缓冲池中取出）
    RmPageHandle page_handle = fetch_page_handle(file_hdr_.first_free_page_no);

    // 更新文件头中的第一个空闲页指针：
    // 将原空闲页的next_free_page_no作为新的链表头
    // （相当于从空闲链表中取出第一个节点）
    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;

    // 将更新后的文件头立即持久化到磁盘（确保数据一致性）
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_,
                              sizeof(file_hdr_));

    return page_handle;
}

/**
 * @description: 释放页面句柄（当页面从满变为不满时调用）
 * @param {RmPageHandle} &page_handle 要释放的页面句柄
 *
 * 实现逻辑：
 * 1. 将页面重新链接到空闲页链表头部
 * 2. 更新文件头中的第一个空闲页指针
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // 只有当页面不在空闲链表中时才需要处理
    // （通过检查next_free_page_no是否为RM_NO_PAGE判断）
    if (page_handle.page_hdr->next_free_page_no == RM_NO_PAGE) {
        // 将当前页面插入空闲链表头部：
        // 1. 当前页面的next指向原空闲链表头
        page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;

        // 2. 文件头的first_free_page_no指向当前页面
        file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;

        // 将更新后的文件头持久化到磁盘
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_,
                                  sizeof(file_hdr_));
    }

    // 注意：这里没有unpin_page操作，由调用者负责
}