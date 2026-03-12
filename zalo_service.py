import requests
import os
import sqlite3

ZALO_TOKEN = os.getenv("ZALO_BOT_TOKEN")

API_URL = "https://bot-api.zapps.me/v1/messages"


def db():
    con = sqlite3.connect("fleet.db")
    con.row_factory = sqlite3.Row
    return con


# =========================
# GỬI TIN NHẮN ZALO
# =========================
def send_zalo(user_id, text):

    payload = {
        "recipient": {"user_id": user_id},
        "message": {"text": text}
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ZALO_TOKEN}"
    }

    try:

        r = requests.post(
            API_URL,
            json=payload,
            headers=headers,
            timeout=10
        )

        print("Zalo status:", r.status_code)
        print("Zalo response:", r.text)

        return r.status_code == 200

    except Exception as e:

        print("Zalo send error:", e)
        return False


# =========================
# LẤY LỆNH XE CỦA TÀI XẾ
# =========================
def get_driver_trip(zalo_id):

    con = db()

    trip = con.execute("""

        SELECT
            v.id,
            v.plate,
            v.start_time,
            v.work_content,
            d.name

        FROM vehicles v
        JOIN drivers d ON v.driver_id = d.id

        WHERE d.zalo_user_id = ?
        AND v.status = 1

    """, (zalo_id,)).fetchone()

    con.close()

    return trip


# =========================
# BOT XỬ LÝ TIN NHẮN
# =========================
def handle_message(user_id, text):

    text = text.lower().strip()

    # help
    if text == "help":

        msg = """
🚗 BOT ĐIỀU XE

lenh  → xem lệnh xe
xong  → kết thúc chuyến
help  → hướng dẫn
"""

        send_zalo(user_id, msg)
        return


    # xem lệnh
    if text == "lenh":

        trip = get_driver_trip(user_id)

        if not trip:

            send_zalo(
                user_id,
                "❌ Bạn hiện không có lệnh điều xe"
            )

            return

        msg = f"""
🚗 LỆNH ĐIỀU XE

Xe: {trip['plate']}
Tài xế: {trip['name']}
Thời gian: {trip['start_time']}

Nội dung:
{trip['work_content']}
"""

        send_zalo(user_id, msg)

        return


    # kết thúc
    if text == "xong":

        trip = get_driver_trip(user_id)

        if not trip:

            send_zalo(
                user_id,
                "❌ Không có chuyến xe đang chạy"
            )

            return

        send_zalo(
            user_id,
            "✅ Đã nhận yêu cầu kết thúc chuyến.\nVui lòng nhập KM trên hệ thống."
        )

        return


    # mặc định
    send_zalo(
        user_id,
        "🤖 Không hiểu lệnh.\nGõ *help* để xem hướng dẫn."
    )