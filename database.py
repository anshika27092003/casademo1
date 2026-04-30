from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

# SQLite Database Setup
DB_FILE = "sqlite:///./settlement_tracking.db"
engine = create_engine(DB_FILE, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class CellChange(Base):
    __tablename__ = "cell_changes"

    id = Column(Integer, primary_key=True, index=True)
    sheet_name = Column(String, index=True)
    cell_reference = Column(String)
    label_name = Column(String)
    old_value = Column(String, nullable=True)
    new_value = Column(String)
    source_table = Column(String, nullable=True) # CK, SP, FWL
    source_id = Column(Integer, nullable=True)   # ID from the related table
    timestamp = Column(DateTime, default=datetime.utcnow)

class CKSecreterial(Base):
    __tablename__ = "ck_secreterial"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    supplier_name = Column(String, nullable=True)
    consignment_number = Column(String, nullable=True)
    invoice_date = Column(String, nullable=True)
    invoice_no = Column(String, nullable=True)
    bill_to = Column(String, nullable=True)
    sub_total = Column(String, nullable=True)
    gst_amount = Column(String, nullable=True)
    total_amount = Column(String)
    remarks = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

class SPTable(Base):
    __tablename__ = "sp_table"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    supplier_name = Column(String, nullable=True)
    clinic_name = Column(String, nullable=True)
    invoice_date = Column(String, nullable=True)
    tax_invoice_number = Column(String, nullable=True)
    sub_total = Column(String, nullable=True)
    gst_amount = Column(String, nullable=True)
    total_amount = Column(String)
    remarks = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

class FWLTable(Base):
    __tablename__ = "fwl_table"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    clinic_name = Column(String, nullable=True)
    total_payable = Column(String)
    remarks = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(bind=engine)
