from flask import Blueprint, render_template, request, redirect, url_for,jsonify, flash
from .models import db, Driver, Farmer, DriverPatti, LotInfo
from datetime import date
main = Blueprint('main', __name__)
import sqlite3
from datetime import datetime


DB_PATH = "app/database/jaikissan.db"  # Adjust if your DB path differs

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn


@main.route('/')
def home():
    return redirect(url_for('main.entry'))

@main.route('/driver_patti', methods=['GET', 'POST'])
def driver_patti():
    if request.method == 'POST':
        conn = get_db_connection()
        conn.execute("""
            INSERT INTO driver_patti (driver_id, transport_rate, date)
            VALUES (?, ?, ?)
        """, (
            int(request.form['driver_id']),
            float(request.form['transport_rate']),
            request.form['date']
        ))
        conn.commit()
        conn.close()
        return redirect(url_for('main.driver_patti'))

    conn = get_db_connection()
    # Get enriched driver pattis using JOIN and aggregation
    rows = conn.execute("""
        SELECT 
            dp.id AS patti_id,
            d.name AS driver_name,
            d.vehicle_number,
            d.village,
            dp.transport_rate,
            dp.date,
            IFNULL(SUM(l.jk_boxes), 0) AS jk_boxes,
            IFNULL(SUM(l.other_boxes), 0) AS other_boxes,
            IFNULL(SUM(l.jk_boxes + l.other_boxes), 0) AS total_boxes,
            ROUND(IFNULL(SUM(l.jk_boxes + l.other_boxes), 0) * dp.transport_rate, 2) AS cost
        FROM driver_patti dp
        JOIN driver d ON dp.driver_id = d.id
        LEFT JOIN lot_info l ON dp.id = l.driver_patti_id
        GROUP BY dp.id
        ORDER BY dp.date DESC
    """).fetchall()
    drivers = conn.execute("SELECT * FROM driver ORDER BY name").fetchall()
    conn.close()

    today = date.today().isoformat()
    return render_template('driver_patti.html', pattis=rows, drivers=drivers, today=today)


@main.route('/lot_info', methods=['GET', 'POST'])
def lot_info():
    if request.method == 'POST':
        jk_boxes = int(request.form['jk_boxes'])
        other_boxes = int(request.form['other_boxes'])
        total_boxes = jk_boxes + other_boxes
        rate = float(request.form['transport_rate'])
        # Step 1: Get selected date
        lot_date = request.form['date']

        # Step 2: Format DDMMYY
        from datetime import datetime
        prefix = datetime.strptime(lot_date, "%Y-%m-%d").strftime("%d%m%y")

        # Step 3: Find last used number for the date
        conn = get_db_connection()
        row = conn.execute("""
            SELECT lot_number FROM lot_info
            WHERE date = ?
            AND lot_number LIKE ?
            ORDER BY lot_number DESC
            LIMIT 1
        """, (lot_date, f"{prefix}%")).fetchone()
        conn.close()

        if row:
            last_number = int(row["lot_number"][-3:])
            next_number = last_number + 1
        else:
            next_number = 1

        lot_number = f"{prefix}{next_number:03d}"

        lot = LotInfo(
            driver_patti_id=int(request.form['driver_patti_id']),
            farmer_id=int(request.form['farmer_id']),
            lot_number=request.form['lot_number'],
            jk_boxes=jk_boxes,
            other_boxes=other_boxes,
            transport_rate=rate,
            total_boxes=total_boxes,
            transport_amount=total_boxes * rate,
            date=request.form['date']
        )
        db.session.add(lot)
        db.session.commit()
        return redirect(url_for('main.lot_info'))

    lots = LotInfo.query.all()
    pattis = DriverPatti.query.all()
    farmers = Farmer.query.all()
    today = date.today().isoformat()
    return render_template('lot_info.html', lots=lots, pattis=pattis, farmers=farmers, today=today)
@main.route('/add_driver', methods=['GET', 'POST'])
def add_driver():
    if request.method == 'POST':
        name = request.form['name']
        vehicle_number = request.form['vehicle_number']
        village = request.form['village']
        default_rate = float(request.form['default_rate'])

        new_driver = Driver(
            name=name,
            vehicle_number=vehicle_number,
            village=village,
            default_rate=default_rate
        )
        db.session.add(new_driver)
        db.session.commit()
        return redirect(url_for('main.driver_patti'))

    return render_template('add_driver.html')
@main.route("/add_driver_modal", methods=["POST"])
def add_driver_modal():
    name = request.form.get("name").strip()
    vehicle = request.form.get("vehicle_number").strip()
    village = request.form.get("village").strip()  # ✅ New field
    rate = float(request.form.get("default_rate"))

    db = get_db_connection()
    cur = db.cursor()
    cur.execute("INSERT INTO driver (name, vehicle_number, village, default_rate) VALUES (?, ?, ?, ?)",
                (name, vehicle, village, rate))
    db.commit()

    return redirect(url_for('main.entry'))

@main.route("/api/farmers")
def api_farmers():
    q = request.args.get("q", "")
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT id, name, phone FROM farmer
        WHERE name LIKE ? OR phone LIKE ?
        ORDER BY name LIMIT 10
    """, (f"%{q}%", f"%{q}%")).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])

@main.route("/api/drivers")
def api_drivers():
    q = request.args.get("q", "")
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT id, name, vehicle_number,default_rate FROM driver
        WHERE name LIKE ? OR vehicle_number LIKE ?
        LIMIT 10
    """, (f"%{q}%", f"%{q}%")).fetchall()
    conn.close()
    print("Here")
    return jsonify([dict(row) for row in rows])


@main.route('/add_farmer_modal', methods=['POST'])
def add_farmer_modal():
    name = request.form['name']
    phone = request.form.get('phone', '')

    conn = get_db_connection()
    conn.execute("INSERT INTO farmer (name, phone) VALUES (?, ?)", (name, phone))
    conn.commit()
    conn.close()
    return redirect(url_for('main.entry'))
    # return redirect(url_for('main.lot_info'))

@main.route("/api/pattis")
def api_pattis():
    q = request.args.get("q", "")
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT dp.id, dp.transport_rate, d.name AS driver_name, d.vehicle_number
        FROM driver_patti dp
        LEFT JOIN driver d ON dp.driver_id = d.id
        WHERE dp.date = ?
        ORDER BY dp.id DESC
    """, (q,)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


# @main.route("/api/next_lot_number")
# def api_next_lot_number():
#     lot_date = request.args.get("date")
#     if not lot_date:
#         return jsonify({"error": "Date is required"}), 400

#     prefix = datetime.strptime(lot_date, "%Y-%m-%d").strftime("%d%m%y")

#     conn = get_db_connection()
#     row = conn.execute("""
#         SELECT lot_number FROM lot_info
#         WHERE date = ? AND lot_number LIKE ?
#         ORDER BY lot_number DESC LIMIT 1
#     """, (lot_date, f"{prefix}%")).fetchone()
#     conn.close()

#     if row:
#         last_number = int(row["lot_number"][-3:])
#         next_number = last_number + 1
#     else:
#         next_number = 1

#     return jsonify({"lot_number": f"{prefix}{next_number:03d}"})


@main.route("/api/lots")
def api_lots():
    date_filter = request.args.get("date")
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT l.*, f.name AS farmer_name
        FROM lot_info l
        LEFT JOIN farmer f ON l.farmer_id = f.id
        WHERE l.date = ?
        ORDER BY l.lot_number
    """, (date_filter,)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])

@main.route("/api/driver_pattis")
def api_driver_pattis():
    date_filter = request.args.get("date")
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT 
            dp.id AS patti_id,
            d.name AS driver_name,
            d.vehicle_number,
            d.village,
            dp.transport_rate,
            dp.date,
            IFNULL(SUM(l.jk_boxes), 0) AS jk_boxes,
            IFNULL(SUM(l.other_boxes), 0) AS other_boxes,
            IFNULL(SUM(l.jk_boxes + l.other_boxes), 0) AS total_boxes,
            ROUND(IFNULL(SUM(l.jk_boxes + l.other_boxes), 0) * dp.transport_rate, 2) AS total_transport_cost
        FROM driver_patti dp
        JOIN driver d ON dp.driver_id = d.id
        LEFT JOIN lot_info l ON dp.id = l.driver_patti_id
        WHERE dp.date = ?
        GROUP BY dp.id
        ORDER BY dp.date DESC
    """, (date_filter,)).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # requires Python 3.9+

@main.route("/entry", methods=["GET", "POST"])
def entry():
    if request.method == "GET":
        ist = ZoneInfo("Asia/Kolkata")  # Use pytz.timezone("Asia/Kolkata") for older Python
        now = datetime.now(ist)

        if now.hour < 10:
            entry_date = now.date()
        else:
            entry_date = now.date() + timedelta(days=1)

        return render_template("entry.html", today=entry_date.strftime("%Y-%m-%d"))

    # POST logic remains the same
    date_val = request.form["date"]
    driver_id = request.form.get("driver_id")
    transport_rate = float(request.form["transport_rate"])

    if not driver_id:
        flash("Please select a valid driver from the list.")
        return redirect(url_for('main.entry'))

    db_conn = get_db_connection()
    cur = db_conn.cursor()

    cur.execute("""
        INSERT INTO driver_patti (driver_id, transport_rate, date)
        VALUES (?, ?, ?)
    """, (driver_id, transport_rate, date_val))
    driver_patti_id = cur.lastrowid

    prefix = datetime.strptime(date_val, "%Y-%m-%d").strftime("%d%m")
    cur.execute("""
        SELECT lot_number FROM lot_info
        WHERE lot_number LIKE ?
        ORDER BY lot_number DESC LIMIT 1
    """, (f"{prefix}%",))
    row = cur.fetchone()
    last_num = int(row["lot_number"][-3:]) if row else 0

    farmer_ids = request.form.getlist("farmer_id[]")
    jk_boxes = request.form.getlist("jk_boxes[]")
    other_boxes = request.form.getlist("other_boxes[]")

    for i in range(len(farmer_ids)):
        if not farmer_ids[i].strip():
            flash(f"Lot {i+1}: Please select a valid farmer from the list.")
            return redirect(url_for('main.entry'))

        lot_number = f"{prefix}{last_num + i + 1:03d}"
        cur.execute("""
            INSERT INTO lot_info (driver_patti_id, farmer_id, lot_number, jk_boxes, other_boxes)
            VALUES (?, ?, ?, ?, ?)
        """, (
            driver_patti_id,
            farmer_ids[i],
            lot_number,
            int(jk_boxes[i]),
            int(other_boxes[i])
        ))
    
    unique_pairs = set(zip([driver_patti_id]*len(farmer_ids), farmer_ids))
    for dp_id, f_id in unique_pairs:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO farmer_patti (driver_patti_id, farmer_id)
                VALUES (?, ?)
            """, (dp_id, f_id))
        except Exception as e:
            print("Farmer Patti insert error:", e)

    db_conn.commit()
    return redirect(url_for("main.entry", patti_id=driver_patti_id))


@main.route("/api/next_lot_number")
def get_next_lot_number():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "Date required"}), 400

    prefix = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d%m")

    db = get_db_connection()
    cur = db.cursor()
    cur.execute(
        "SELECT lot_number FROM lot_info WHERE lot_number LIKE ? ORDER BY lot_number DESC LIMIT 1",
        (f"{prefix}%",)
    )
    row = cur.fetchone()
    last = int(row["lot_number"][-3:]) if row else 0

    return jsonify({"prefix": prefix, "last": last})


@main.route("/receipt/<int:patti_id>")
def show_receipt(patti_id):
    db = get_db_connection()
    cur = db.cursor()

    # Driver patti header
    patti = cur.execute("""
        SELECT dp.id, dp.date, d.name AS driver_name, d.vehicle_number, d.village, dp.transport_rate
        FROM driver_patti dp
        JOIN driver d ON d.id = dp.driver_id
        WHERE dp.id = ?
    """, (patti_id,)).fetchone()

    # All lots under this patti (include farmer_id for grouping)
    lots = cur.execute("""
        SELECT li.lot_number,
               f.name AS farmer_name,
               f.id   AS farmer_id,
               li.jk_boxes,
               li.other_boxes
        FROM lot_info li
        JOIN farmer f ON f.id = li.farmer_id
        WHERE li.driver_patti_id = ?
        ORDER BY li.lot_number
    """, (patti_id,)).fetchall()

    total_jk = sum(int(l["jk_boxes"] or 0) for l in lots)
    total_other = sum(int(l["other_boxes"] or 0) for l in lots)
    total_cost = (total_jk + total_other) * float(patti["transport_rate"])

    # Pull farmer_patti ids and stitch lots to each farmer
    farmer_rows = cur.execute("""
        SELECT fp.id AS farmer_patti_id, f.id AS farmer_id, f.name AS farmer_name
        FROM farmer_patti fp
        JOIN farmer f ON f.id = fp.farmer_id
        WHERE fp.driver_patti_id = ?
        ORDER BY f.name
    """, (patti_id,)).fetchall()

    farmer_groups = []
    for fr in farmer_rows:
        flots = [l for l in lots if l["farmer_id"] == fr["farmer_id"]]
        farmer_groups.append({
            "farmer_patti_id": fr["farmer_patti_id"],
            "farmer_name": fr["farmer_name"],
            "lots": [{
                "lot_number": l["lot_number"],
                "boxes": int(l["jk_boxes"] or 0) + int(l["other_boxes"] or 0),
            } for l in flots],
            "total": sum(int(l["jk_boxes"] or 0) + int(l["other_boxes"] or 0) for l in flots),
        })

    return render_template(
        "receipt.html",
        patti=patti,
        lots=lots,
        total_jk=total_jk,
        total_other=total_other,
        total_cost=total_cost,
        farmer_groups=farmer_groups
    )


@main.route("/receipt/farmer_pattis/<int:driver_patti_id>")
def farmer_patti_receipts(driver_patti_id):
    db = get_db_connection()
    cur = db.cursor()

    rows = cur.execute("""
        SELECT 
            li.lot_number,
            COALESCE(li.jk_boxes, 0) AS jk_boxes,
            COALESCE(li.other_boxes, 0) AS other_boxes,
            dp.date,
            d.village,                         -- from driver table
            f.name AS farmer_name,
            f.id   AS farmer_id,
            fp.id  AS farmer_patti_id          -- from farmer_patti table
        FROM lot_info li
        JOIN driver_patti dp ON li.driver_patti_id = dp.id
        JOIN driver d       ON dp.driver_id = d.id
        JOIN farmer f       ON f.id = li.farmer_id
        LEFT JOIN farmer_patti fp 
               ON fp.driver_patti_id = li.driver_patti_id 
              AND fp.farmer_id       = li.farmer_id
        WHERE li.driver_patti_id = ?
        ORDER BY f.name, li.lot_number
    """, (driver_patti_id,)).fetchall()

    # Group: one record per farmer with metadata + lots
    farmer_lots = {}
    for r in rows:
        fid = r["farmer_id"]
        if fid not in farmer_lots:
            farmer_lots[fid] = {
                "farmer_name": r["farmer_name"],
                "village": r["village"],
                "farmer_patti_id": r["farmer_patti_id"],
                "date": r["date"],
                "lots": []
            }
        farmer_lots[fid]["lots"].append(r)

    return render_template("farmer_patti_receipt.html", farmer_lots=farmer_lots)


@main.route("/edit_patti/<int:patti_id>", methods=["GET", "POST"])
def edit_patti(patti_id):
    conn = get_db_connection()
    if request.method == "POST":
        # --- Update patti header ---
        date_val = request.form.get("date")
        rate_val = float(request.form.get("transport_rate") or 0)
        conn.execute(
            "UPDATE driver_patti SET date = ?, transport_rate = ? WHERE id = ?",
            (date_val, rate_val, patti_id),
        )

        # --- Update all lots in one go ---
        lot_ids = request.form.getlist("lot_id[]")
        lot_numbers = request.form.getlist("lot_number[]")
        jk_list = request.form.getlist("jk_boxes[]")
        other_list = request.form.getlist("other_boxes[]")

        for lot_id, lot_no, jk, ot in zip(lot_ids, lot_numbers, jk_list, other_list):
            jk_i = int(jk or 0)
            ot_i = int(ot or 0)
            conn.execute("""
                UPDATE lot_info
                SET lot_number = ?, jk_boxes = ?, other_boxes = ?
                WHERE id = ?
            """, (lot_no.strip(), jk_i, ot_i, int(lot_id)))

        conn.commit()
        conn.close()
        flash("Driver Patti and lots updated.")
        return redirect(url_for("main.edit_patti", patti_id=patti_id))

    # --- GET flow (unchanged) ---
    patti = conn.execute("""
        SELECT dp.id, dp.date AS patti_date, dp.transport_rate AS patti_rate, d.name AS driver_name
        FROM driver_patti dp
        JOIN driver d ON dp.driver_id = d.id
        WHERE dp.id = ?
    """, (patti_id,)).fetchone()

    lots = conn.execute("""
        SELECT li.id AS lot_id, f.name AS farmer_name, li.lot_number, li.jk_boxes, li.other_boxes
        FROM lot_info li
        LEFT JOIN farmer f ON li.farmer_id = f.id
        WHERE li.driver_patti_id = ?
        ORDER BY li.lot_number
    """, (patti_id,)).fetchall()

    conn.close()
    return render_template("edit_patti.html", patti=patti, lots=lots)

@main.route("/update_lot/<int:lot_id>", methods=["POST"])
def update_lot(lot_id):
    lot_number = request.form.get('lot_number')
    jk = int(request.form.get('jk_boxes') or 0)
    other = int(request.form.get('other_boxes') or 0)

    conn = get_db_connection()
    conn.execute("""
        UPDATE lot_info
        SET lot_number = ?, jk_boxes = ?, other_boxes = ?
        WHERE id = ?
    """, (lot_number, jk, other, lot_id))
    conn.commit()
    conn.close()

    flash("Lot updated.")
    return redirect(request.referrer or '/')


from datetime import date

@main.route('/lots', methods=['GET'])
def show_lots():
    selected_date = request.args.get('date', date.today().isoformat())

    conn = get_db_connection()
    lots = conn.execute("""
        SELECT 
            dp.id as driver_patti_id,
            l.lot_number,
            d.name AS driver_name,
            d.village AS village,
            f.name AS farmer_name,
            l.jk_boxes,
            l.other_boxes,
            dp.transport_rate,
            (l.jk_boxes + l.other_boxes) * dp.transport_rate AS total_amount
        FROM lot_info l
        JOIN driver_patti dp ON l.driver_patti_id = dp.id
        JOIN driver d ON dp.driver_id = d.id
        JOIN farmer f ON l.farmer_id = f.id
        WHERE dp.date = ?
        ORDER BY dp.id ASC, l.lot_number ASC
    """, (selected_date,)).fetchall()
    conn.close()

    return render_template('lots.html', lots=lots, selected_date=selected_date)


@main.route("/api/buyers")
def api_buyers():
    q = request.args.get("q", "").strip()
    conn = get_db_connection()
    cur = conn.cursor()
    if q:
        cur.execute("""
            SELECT id, name, nick_name
            FROM buyers
            WHERE nick_name LIKE ? OR name LIKE ?
            ORDER BY nick_name, name
            LIMIT 20
        """, (f"%{q}%", f"%{q}%"))
    else:
        cur.execute("""
            SELECT id, name, nick_name
            FROM buyers
            ORDER BY nick_name, name
            LIMIT 50
        """)
    rows = cur.fetchall()
    conn.close()

    # Return {id, label} where label = "Nick (Name)" or just one of them
    out = []
    for r in rows:
        name = r["name"] or ""
        nick = r["nick_name"] or ""
        if nick and name:
            label = f"{nick} ({name})"
        else:
            label = nick or name
        out.append({"id": r["id"], "label": label})
    return jsonify(out)


@main.route("/api/buyers", methods=["POST"])
def api_create_buyer():
    data = request.get_json(silent=True) or request.form
    name = (data.get("name") or "").strip()
    nick = (data.get("nick_name") or "").strip()
    location = (data.get("location") or "").strip()
    address = (data.get("address") or "").strip()
    phone = (data.get("phone_number") or "").strip()

    if not (name or nick):
        return jsonify(success=False, error="Provide at least Name or Nick Name"), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO buyers (name, nick_name, location, address, phone_number)
        VALUES (?, ?, ?, ?, ?)
    """, (name, nick, location, address, phone))
    new_id = cur.lastrowid
    conn.commit()
    conn.close()

    label = f"{nick} ({name})" if (nick and name) else (nick or name)
    return jsonify(success=True, id=new_id, label=label)

@main.route("/add_buyer_modal", methods=["POST"])
def add_buyer_modal():
    name = (request.form.get("name") or "").strip()
    nick = (request.form.get("nick_name") or "").strip()
    location = (request.form.get("location") or "").strip()
    address = (request.form.get("address") or "").strip()
    phone = (request.form.get("phone_number") or "").strip()

    if not (name or nick):
        flash("Please enter at least Nick Name or Name for the buyer.")
        return redirect(request.referrer or url_for("main.sales"))

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO buyers (name, nick_name, location, address, phone_number)
        VALUES (?, ?, ?, ?, ?)
    """, (name, nick, location, address, phone))
    conn.commit()
    conn.close()

    flash("Buyer added.")
    return redirect(request.referrer or url_for("main.sales"))

@main.route("/sales", methods=["GET", "POST"])
def sales():
    if request.method == "POST":
        date_val = request.form.get("date")
        if not date_val:
            flash("Please select a date.")
            return redirect(url_for("main.sales"))

        conn = get_db_connection()
        cur = conn.cursor()

        # 1) Collect only rows that have BOTH buyer_id and rate
        lot_ids_to_check = []
        parsed_rows = {}  # lot_id -> {"buyer_id":int, "rate":float, "less":int}
        for key in request.form.keys():
            if not key.startswith("rate_"):
                continue
            lot_id = key.split("_", 1)[1]

            rate_raw = (request.form.get(f"rate_{lot_id}") or "").strip()
            buyer_id_raw = (request.form.get(f"buyer_id_{lot_id}") or "").strip()
            less_raw = (request.form.get(f"less_{lot_id}") or "").strip()

            if not (rate_raw and buyer_id_raw):
                continue  # require both to consider

            try:
                rate = float(rate_raw)
                buyer_id = int(buyer_id_raw)
                less = int(less_raw) if less_raw else 0
            except ValueError:
                continue

            lot_ids_to_check.append(lot_id)
            parsed_rows[int(lot_id)] = {"buyer_id": buyer_id, "rate": rate, "less": less}

        created = 0
        updated = 0
        unchanged = 0

        if lot_ids_to_check:
            # 2) Load existing sales for these lots
            placeholders = ",".join(["?"] * len(lot_ids_to_check))
            cur.execute(f"""
                SELECT lot_id, buyer_id, rate, less
                FROM sales
                WHERE lot_id IN ({placeholders})
            """, lot_ids_to_check)
            existing_map = {
                int(r["lot_id"]): {
                    "buyer_id": r["buyer_id"],
                    "rate": float(r["rate"]),
                    "less": int(r["less"]),
                }
                for r in cur.fetchall()
            }
        else:
            existing_map = {}

        # 3) Insert new / update changed only
        EPS = 1e-9  # float tolerance
        for lot_id, vals in parsed_rows.items():
            ex = existing_map.get(lot_id)
            if ex is None:
                cur.execute("""
                    INSERT INTO sales (lot_id, buyer_id, rate, less)
                    VALUES (?, ?, ?, ?)
                """, (lot_id, vals["buyer_id"], vals["rate"], vals["less"]))
                created += 1
            else:
                changed = (
                    ex["buyer_id"] != vals["buyer_id"] or
                    abs(ex["rate"] - vals["rate"]) > EPS or
                    ex["less"] != vals["less"]
                )
                if changed:
                    cur.execute("""
                        UPDATE sales
                        SET buyer_id = ?, rate = ?, less = ?
                        WHERE lot_id = ?
                    """, (vals["buyer_id"], vals["rate"], vals["less"], lot_id))
                    updated += 1
                else:
                    unchanged += 1

        conn.commit()
        conn.close()

        msg = f"Saved {created + updated} sale(s): {created} new, {updated} updated, {unchanged} unchanged for {date_val}."
        xrw = request.headers.get("X-Requested-With", "")
        if xrw in ("fetch", "XMLHttpRequest"):
            return jsonify(success=True, created=created, updated=updated, unchanged=unchanged, message=msg)

        flash(msg)
        return redirect(url_for("main.sales", date=date_val))

    # ---- GET ----
    today_str = date.today().isoformat()
    date_val = request.args.get("date", today_str)

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            li.id AS lot_id,
            li.lot_number,
            COALESCE(li.jk_boxes, 0) + COALESCE(li.other_boxes, 0) AS total_boxes
        FROM lot_info li
        JOIN driver_patti dp ON li.driver_patti_id = dp.id
        WHERE dp.date = ?
        ORDER BY li.lot_number
    """, (date_val,))
    lots = cur.fetchall()

    sales_map = {}
    if lots:
        lot_ids = [str(r["lot_id"]) for r in lots]
        placeholders = ",".join(["?"] * len(lot_ids))
        cur.execute(f"""
            SELECT lot_id, buyer_id, rate, less
            FROM sales
            WHERE lot_id IN ({placeholders})
        """, lot_ids)
        for r in cur.fetchall():
            sales_map[r["lot_id"]] = {
                "buyer_id": r["buyer_id"],
                "rate": r["rate"],
                "less": r["less"],
            }

    conn.close()
    return render_template("sales.html", date_val=date_val, lots=lots, sales_map=sales_map)


@main.route("/receipt/driver_patti/<int:patti_id>")
def driver_patti_receipt(patti_id):
    db = get_db_connection()
    cur = db.cursor()

    patti = cur.execute("""
        SELECT dp.id, dp.date, d.name AS driver_name, d.vehicle_number, d.village, dp.transport_rate
        FROM driver_patti dp
        JOIN driver d ON d.id = dp.driver_id
        WHERE dp.id = ?
    """, (patti_id,)).fetchone()

    lots = cur.execute("""
        SELECT li.lot_number, f.name AS farmer_name, li.jk_boxes, li.other_boxes
        FROM lot_info li
        JOIN farmer f ON f.id = li.farmer_id
        WHERE li.driver_patti_id = ?
        ORDER BY li.lot_number
    """, (patti_id,)).fetchall()

    total_jk = sum(int(l["jk_boxes"] or 0) for l in lots)
    total_other = sum(int(l["other_boxes"] or 0) for l in lots)
    total_cost = (total_jk + total_other) * float(patti["transport_rate"])

    db.close()
    return render_template(
        "driver_patti_receipt.html",
        patti=patti,
        lots=lots,
        total_jk=total_jk,
        total_other=total_other,
        total_cost=total_cost
    )

from flask import Blueprint, render_template, request, make_response
from datetime import datetime, timedelta
import csv, io

# If you already have `main = Blueprint("main", __name__)` in routes.py, reuse it.
# Otherwise, uncomment below to create a standalone blueprint and register in app factory.
# main = Blueprint("main", __name__)



@main.route("/reports")
def reports_home():
    now = datetime.now()
    default_date = (now.date() if now.hour < 10 else (now.date() + timedelta(days=1))).strftime("%Y-%m-%d")
    return render_template("reports_home.html", default_date=default_date)

@main.route("/reports/day-sheet")
def report_day_sheet():
    date_str = request.args.get("date", "")
    rows = []
    totals = {"boxes": 0, "less": 0, "net_boxes": 0, "gross": 0}

    if not date_str:
        return render_template("day_sheet.html", date_str=date_str, rows=rows, totals=totals)

    conn = get_db_connection()
    cur = conn.cursor()

    sql = """
    SELECT
        li.lot_number                                 AS lot_number,
        COALESCE(li.jk_boxes, 0) + COALESCE(li.other_boxes, 0) AS boxes,
        COALESCE(s.less, 0)                           AS less,
        COALESCE(s.rate, 0)                           AS rate,
        b.name                                        AS buyer,
        (COALESCE(s.rate,0) * (((COALESCE(li.jk_boxes,0)+COALESCE(li.other_boxes,0)) - COALESCE(s.less,0)))) AS total,
        f.name                                        AS farmer_name,
        d.name                                        AS driver_name,
        d.vehicle_number                              AS vehicle_no,
        d.village                                     AS village
    FROM lot_info li
    JOIN driver_patti dp ON dp.id = li.driver_patti_id
    LEFT JOIN sales s    ON s.lot_id = li.id
    LEFT JOIN buyers b   ON b.id = s.buyer_id
    LEFT JOIN farmer f   ON f.id = li.farmer_id
    LEFT JOIN driver d   ON d.id = dp.driver_id
    WHERE dp.date = ?
    ORDER BY li.lot_number ASC;
    """
    cur.execute(sql, (date_str,))
    db_rows = cur.fetchall()

    for r in db_rows:
        boxes = r[1] or 0
        less  = r[2] or 0
        rate  = r[3] or 0
        net_boxes = boxes - less
        total = rate * net_boxes

        rows.append({
            "lot_number": r[0],
            "boxes": boxes,
            "less": less,
            "rate": rate,
            "buyer": r[4] or "",
            "total": total,
            "farmer_name": r[6] or "",
            "driver_name": r[7] or "",
            "vehicle_no": r[8] or "",
            "village": r[9] or "",
        })
        totals["boxes"] += boxes
        totals["less"] += less
        totals["net_boxes"] += net_boxes
        totals["gross"] += total

    conn.close()
    return render_template("day_sheet.html", date_str=date_str, rows=rows, totals=totals)

@main.route("/reports/day-sheet/export")
def report_day_sheet_export():
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            li.lot_number,
            COALESCE(li.jk_boxes, 0) + COALESCE(li.other_boxes, 0) AS boxes,
            COALESCE(s.less, 0) AS less,
            COALESCE(s.rate, 0) AS rate,
            b.name AS buyer,
            (COALESCE(s.rate,0) * (((COALESCE(li.jk_boxes,0)+COALESCE(li.other_boxes,0)) - COALESCE(s.less,0)))) AS total,
            f.name AS farmer_name,
            d.name AS driver_name,
            d.vehicle_number AS vehicle_no,
            d.village AS village
        FROM lot_info li
        JOIN driver_patti dp ON dp.id = li.driver_patti_id
        LEFT JOIN sales s    ON s.lot_id = li.id
        LEFT JOIN buyers b   ON b.id = s.buyer_id
        LEFT JOIN farmer f   ON f.id = li.farmer_id
        LEFT JOIN driver d   ON d.id = dp.driver_id
        WHERE dp.date = ?
        ORDER BY li.lot_number ASC;
        """,
        (date_str,)
    )
    rows = cur.fetchall()
    conn.close()

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Date", date_str])
    w.writerow(["Lot number","Boxes (JK+Other)","Less","Rate","Buyer","Total (Rate*(Boxes-Less))","Farmer","Driver","Vehicle No","Village"])
    for r in rows:
        boxes = (r[1] or 0)
        less  = (r[2] or 0)
        rate  = (r[3] or 0)
        total = rate * (boxes - less)
        w.writerow([r[0], boxes, less, rate, r[4] or "", total, r[6] or "", r[7] or "", r[8] or "", r[9] or ""])

    resp = make_response(out.getvalue())
    resp.headers["Content-Disposition"] = f"attachment; filename=day_sheet_{date_str}.csv"
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    return resp

@main.route("/reports/buyer")
def report_buyer():
    date_str = request.args.get("date", "")
    rows = []
    totals = {"boxes": 0, "less": 0, "net_boxes": 0, "amount": 0.0}
    if not date_str:
        return render_template("buyer_report.html", date_str=date_str, rows=rows, totals=totals)

    conn = get_db_connection()
    cur = conn.cursor()

    sql = """
    WITH lot_level AS (
        SELECT
            b.name AS buyer_name,
            (COALESCE(li.jk_boxes,0) + COALESCE(li.other_boxes,0)) AS boxes,
            COALESCE(s.less,0) AS less,
            COALESCE(s.rate,0) AS rate,
            (COALESCE(li.jk_boxes,0) + COALESCE(li.other_boxes,0) - COALESCE(s.less,0)) AS net_boxes,
            (COALESCE(s.rate,0) * (COALESCE(li.jk_boxes,0) + COALESCE(li.other_boxes,0) - COALESCE(s.less,0))) AS amount
        FROM lot_info li
        JOIN driver_patti dp ON dp.id = li.driver_patti_id
        JOIN sales s        ON s.lot_id = li.id
        LEFT JOIN buyers b  ON b.id = s.buyer_id
        WHERE dp.date = ?
    )
    SELECT
        buyer_name,
        SUM(boxes)      AS total_boxes,
        SUM(less)       AS total_less,
        SUM(net_boxes)  AS total_net_boxes,
        CASE WHEN SUM(net_boxes) > 0 THEN SUM(amount)/SUM(net_boxes) ELSE 0 END AS avg_rate,
        SUM(amount)     AS total_amount
    FROM lot_level
    GROUP BY buyer_name
    ORDER BY buyer_name COLLATE NOCASE;
    """
    cur.execute(sql, (date_str,))
    for buyer_name, total_boxes, total_less, total_net_boxes, avg_rate, total_amount in cur.fetchall():
        rows.append({
            "buyer_name": buyer_name or "—",
            "boxes": total_boxes or 0,
            "less": total_less or 0,
            "net_boxes": total_net_boxes or 0,
            "rate": float(avg_rate or 0.0),
            "amount": float(total_amount or 0.0),
        })
        totals["boxes"] += total_boxes or 0
        totals["less"] += total_less or 0
        totals["net_boxes"] += total_net_boxes or 0
        totals["amount"] += float(total_amount or 0.0)

    conn.close()
    return render_template("buyer_report.html", date_str=date_str, rows=rows, totals=totals)

@main.route("/reports/buyer/export")
def report_buyer_export():
    date_str = request.args.get("date")
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        WITH lot_level AS (
            SELECT
                b.name AS buyer_name,
                (COALESCE(li.jk_boxes,0) + COALESCE(li.other_boxes,0)) AS boxes,
                COALESCE(s.less,0) AS less,
                (COALESCE(li.jk_boxes,0) + COALESCE(li.other_boxes,0) - COALESCE(s.less,0)) AS net_boxes,
                (COALESCE(s.rate,0) * (COALESCE(li.jk_boxes,0) + COALESCE(li.other_boxes,0) - COALESCE(s.less,0))) AS amount
            FROM lot_info li
            JOIN driver_patti dp ON dp.id = li.driver_patti_id
            JOIN sales s        ON s.lot_id = li.id
            LEFT JOIN buyers b  ON b.id = s.buyer_id
            WHERE dp.date = ?
        )
        SELECT buyer_name, SUM(boxes), SUM(less), SUM(net_boxes),
               CASE WHEN SUM(net_boxes) > 0 THEN SUM(amount)/SUM(net_boxes) ELSE 0 END AS avg_rate,
               SUM(amount)
        FROM lot_level
        GROUP BY buyer_name
        ORDER BY buyer_name COLLATE NOCASE;
        """,
        (date_str,)
    )
    rows = cur.fetchall()
    conn.close()

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Date", date_str])
    w.writerow(["Buyer","Total Boxes","Less","Net Boxes","Rate","Amount"])
    t_boxes=t_less=t_net=0
    t_amt=0.0
    for buyer, boxes, less, net, rate, amt in rows:
        w.writerow([buyer or "—", boxes or 0, less or 0, net or 0, float(rate or 0.0), float(amt or 0.0)])
        t_boxes += boxes or 0
        t_less  += less or 0
        t_net   += net or 0
        t_amt   += float(amt or 0.0)
    w.writerow(["TOTAL", t_boxes, t_less, t_net, "", t_amt])

    resp = make_response(out.getvalue())
    resp.headers["Content-Disposition"] = f"attachment; filename=buyer_report_{date_str}.csv"
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    return resp