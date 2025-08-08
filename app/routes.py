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
    return redirect(url_for('main.driver_patti'))

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

    db_conn.commit()
    return redirect(url_for("main.entry"))


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
    """, (patti_id,)).fetchall()

    total_jk = sum(l["jk_boxes"] for l in lots)
    total_other = sum(l["other_boxes"] for l in lots)
    total_cost = (total_jk + total_other) * patti["transport_rate"]

    return render_template("receipt.html", patti=patti, lots=lots,
                           total_jk=total_jk, total_other=total_other, total_cost=total_cost)

@main.route("/receipt/farmer_pattis/<int:driver_patti_id>")
def farmer_patti_receipts(driver_patti_id):
    db = get_db_connection()
    cur = db.cursor()

    # Get all lots for this patti with farmer info
    lots = cur.execute("""
        SELECT li.lot_number, li.jk_boxes, li.other_boxes, li.date, f.name as farmer_name, f.id as farmer_id
        FROM lot_info li
        JOIN farmer f ON f.id = li.farmer_id
        WHERE li.driver_patti_id = ?
        ORDER BY f.name, li.lot_number
    """, (driver_patti_id,)).fetchall()

    # Group lots by farmer
    from collections import defaultdict
    farmer_lots = defaultdict(list)
    for lot in lots:
        farmer_lots[lot["farmer_id"]].append(lot)

    return render_template("farmer_receipts.html", farmer_lots=farmer_lots)

@main.route("/edit_patti/<int:patti_id>")
def edit_patti(patti_id):
    conn = get_db_connection()
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
    """, (patti_id,)).fetchall()

    conn.close()
    return render_template("edit_patti.html", patti=patti, lots=lots)


@main.route("/update_driver_patti/<int:patti_id>", methods=['POST'])
def update_driver_patti(patti_id):
    date = request.form.get('date')
    transport_rate = request.form.get('transport_rate')

    conn = get_db_connection()
    conn.execute("""
        UPDATE driver_patti SET date = ?, transport_rate = ? WHERE id = ?
    """, (date, transport_rate, patti_id))
    conn.commit()
    conn.close()

    flash("Driver Patti updated.")
    return redirect(url_for('main.edit_patti', patti_id=patti_id))

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
        ORDER BY dp.date DESC, l.lot_number ASC
    """, (selected_date,)).fetchall()
    conn.close()

    return render_template('lots.html', lots=lots, selected_date=selected_date)


