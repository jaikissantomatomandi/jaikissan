from . import db

class Driver(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    vehicle_number = db.Column(db.String(50))
    village = db.Column(db.String(100))
    default_rate = db.Column(db.Float)

class Farmer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

class DriverPatti(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    driver_id = db.Column(db.Integer, db.ForeignKey('driver.id'))
    transport_rate = db.Column(db.Float)
    date = db.Column(db.String)


class LotInfo(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    driver_patti_id = db.Column(db.Integer, db.ForeignKey('driver_patti.id'))
    farmer_id = db.Column(db.Integer, db.ForeignKey('farmer.id'))
    lot_number = db.Column(db.String(50))
    jk_boxes = db.Column(db.Integer)
    other_boxes = db.Column(db.Integer)
    transport_rate = db.Column(db.Float)
    total_boxes = db.Column(db.Integer)
    transport_amount = db.Column(db.Float)
    date = db.Column(db.String(20))
