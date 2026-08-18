# REFLECTION — Day 18 Lakehouse Lab

**Author:** Tong Van Tien  
**Date:** 2026-08-18

---

## Top 5 Lakehouse Anti-Patterns

Trong quá trình hoàn thành 8 notebooks của Day 18, tôi nhận ra 5 anti-pattern mà team dễ vướng nhất:

### 1. **Small Files Nightmare (NB2, NB6)**
Team hay quên `OPTIMIZE` sau khi streaming ingest, để lại hàng nghìn file 50KB. Chi phí S3 GET tăng vọt vì mỗi query phải list + open nhiều file hơn. **Fix:** Set writer trigger interval ≥ 128MB hoặc chạy `OPTIMIZE` như job bắt buộc hàng ngày.

### 2. **Orphan Files Under Radar (NB6)**
`VACUUM` chỉ xóa file đã tombstone trong transaction log. Crash mid-write tạo file chưa bao giờ được commit — vô hình với vacuum. Team không để ý vì "vacuum đã chạy". **Fix:** Chạy orphan detection sau vacuum (job 3+4 là cặp).

### 3. **Metadata Phình Nhưng Data Không Giảm (NB5, NB6)**
Iceberg `expire_snapshots` xóa metadata nhưng không đụng data files. Team thấy snapshot count giảm, tưởng đã clean xong. **Fix:** Luôn chạy orphan removal sau expiry.

### 4. **Schema Evolution Without Backfill Plan (NB5)**
`ALTER TABLE ... RENAME COLUMN` trong Iceberg an toàn vì field ID còn giữ nguyên, nhưng team hay hoảng khi đọc dữ liệu cũ thấy `tier=null`. Cần document rằng đây là expected behavior, không phải bug.

### 5. **External Vector Index Drift (NB7)**
Khi xóa document khỏi lakehouse (GDPR erasure), external vector DB vẫn chứa dữ liệu đã xóa nếu không có CDC sync. Đây là lỗi bảo mật nghiêm trọng. **Fix:** Dùng Delta CDF để external index subscribe delete events, hoặc keep embeddings inline trong table.

---

## What Worked Well

1. **Medallion Architecture (NB4):** Bronze→Silver→Gold clean separation giúp debug dễ dàng. Mỗi layer có responsibility rõ ràng.

2. **Time Travel (NB3):** MERGE + RESTORE là công cụ rollback mạnh mẽ. `history()` ≥ 5 versions đủ để trace mọi thay đổi.

3. **Iceberg Hidden Partitioning (NB5):** Tự động prune 10x file khi filter trên timestamp — không cần analyst nhớ partition key.

---

## What I'd Do Differently

- Thêm **monitoring dashboard** cho vacuum/orphan metrics ngay từ đầu
- Viết **runbook** cho 4 maintenance jobs như phần bắt buộc của deployment
- Setup **alerting** khi file count vượt ngưỡng thay vì chờ job hàng ngày
