"""
Migration script: Creates and populates LKP_MAIN_FIELDS table on staging.
This table exists on production but is missing from staging.
Data is sourced from the production database screenshot + legacy ASP.NET dropdown.

Usage:
    cd "D:\projects\new EDMS py\EDMS API"
    python create_lkp_main_fields.py
"""
import asyncio
import os
import sys

sys.path.append(os.getcwd())

from database.connection import get_async_connection
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# All search types from production LKP_MAIN_FIELDS table.
# Format: (TYPE_NAME_AR, FIELD_NAME, FORM, FORM_SRCH_FIELD, TYPE_NAME_EN, system_id, SEARCH_FORM, DISPLAY, exact, DATE_FIELD, KEYID)
LEGACY_SEARCH_TYPES = [
    # (TYPE_NAME, FIELD_NAME, FORM, FORM_SRCH_FIELD, TYPE_NAME_EN, SYSTEM_ID, SEARCH_FORM, DISPLAY, EXACT, DATE_FIELD, KEYID)
    ("إتفاقية", "DOCNAME", "SEARCH_S354", "FORM_NAME", "Agreement -S354", 349206699, "S354", "DOCNAME", "*T*", "RTADOCDATE", "Agreement"),
    ("اسم المتعاقد", "VENDOR_NAME", "SEARCH_S068", "FORM_NAME", "VENDOR NAME S068", 399088859, "S068", "VENDOR_NAME", "*T*", "RTADOCDATE", None),
    ("الرقم الوظيفي أو اسم المرشح لوظيفة", "NUM:ID_NO,STR:RTA_PERSON1", "SEARCH_S0500", "FORM_NAME", "Candidate Name or EMP ID-S0500", 322029279, "S0500", "DOCNAME", "T", "RTADOCDATE", None),
    ("المشروع", "PRJNAME", "SEARCH_S068", "FORM_NAME", "Project S068", 429689951, "S068", "PRJNAME", "*T*", "RTADOCDATE", None),
    ("المعايير والمراجع", "ABSTRACT", "SEARCH_G002", "FORM1", "Standards", 363573280, "G002", "ABSTRACT", "*T*", "CREATION_DATE", None),
    ("بحث المحتوى", "FULLTEXT_CONTENT", "DEF_QBE", "FORM_NAME", "Content Search", 319502494, "G001,G002,DEF_PROF", "DOCNAME", "T", "CREATION_DATE", None),
    ("تعميم", "DOCNAME", "SEARCH_G006", "FORM_NAME", "Circular Subject - G006", 348935737, "G006", "DOCNAME", "*T*", "RTADOCDATE", "Circular"),
    ("رخصة قيادة", "NUM:RTA_REF2,STR:RTA_ARABIC_NAME", "SEARCH_S045", "FORM1", "Driving License", 319250506, "S045", "RTA_ARABIC_NAME", "T*", "CREATION_DATE", None),
    ("رقم الأجرة", "PLATE_NO_2", "SEARCH_S061", "FORM1", "Taxi Plate-S061", 321726188, "S061", "RTA_TEXT1", "T", "RTADOCDATE", None),
    ("رقم الإستمارة", "RECEIPT_NO_N", "SEARCH_S043", "FORM1", "Trans ID", 327293728, "S043,S045", "RECEIPT_NO_N", "T", "CREATION_DATE", None),
    ("رقم الرخصة التجارية", "RTA_REF2", "SEARCH_S084", "FORM1", "Trade License-CTA", 320433880, "S084", "TYPENAME", "T", "CREATION_DATE", None),
    ("رقم الصندوق", "RTA_EXTREFNUM", "SEARCH_S068", "FORM_NAME", "box number S068", 399088540, "S068", "VENDOR_NAME", "T", "RTADOCDATE", None),
    ("رقم المتعاقد", "VENDOR_ID", "SEARCH_S068", "FORM_NAME", "CONTRACTOR ID S068", 399089054, "S068", "VENDOR_NAME", "*T*", "RTADOCDATE", None),
    ("رقم المشروع", "X6687", "SEARCH_S068", "FORM_NAME", "Project code S068", 429689725, "S068", "RTA_EXTREFNUM", "*T*", "RTADOCDATE", None),
    ("رقم الملف أو مالك الأجرة", "NUM:RTA_EXTREFNUM,STR:RTA_TEXT1", "SEARCH_S061", "FORM1", "Taxi Owner OR File No-S061", 321726675, "S061", "RTA_TEXT1", "T*", "RTADOCDATE", None),
    ("رقم بطاقة سالك(S301)", "RTA_TEXT3", "SEARCH_S301", "FORM_NAME", "Salik Tag Number(S301)", 391679532, "S301", "RTA_TEXT3", "T", "CREATION_DATE", None),
    ("رقم لوحة المركبة", "NEW_PLATE_NUMBER", "SEARCH_S043", "FORM1", "Plate Number", 319246653, "S043", "CODE", "T", "RENEWAL_DATE", None),
    ("سالك الملف المروري(S301)", "RTA_NAME", "SEARCH_S301", "FORM_NAME", "Salik Traffic File No(S301)", 391680219, "S301", "RTA_NAME", "T", "CREATION_DATE", None),
    ("سالك رقم الحساب(S301)", "RTA_TEXT1", "SEARCH_S301", "FORM_NAME", "Salik Account No(S301)", 391679212, "S301", "RTA_TEXT1", "T", "CREATION_DATE", None),
    ("شركة", "DOCNAME", "DEF_QBE", "FORM_NAME", "Company", 319081476, "DEF_PROF", "DOCNAME", "*T*", "RTADOCDATE", None),
    ("شيك آجل-اسم الشركة", "DOCNAME", "SEARCH_S014", "FORM1", "PDC-Company", 320172515, "S016", "DOCNAME", "T*", "RTADOCDATE", None),
    ("شيك آجل-رقم الفاتورة", "RTA_EXTREFNUM", "SEARCH_S014", "FORM1", "PDC-Invoice Number", 320172572, "S016", "DOCNAME", "T", "RTADOCDATE", None),
    ("شيكات-رقم الإيصال", "RTA_TEXT1", "SEARCH_S014", "FORM1", "Check-Voucher Number", 320170210, "S014", "DOCNAME", "T", "RTADOCDATE", "choque"),
    ("شيكات-رقم الشيك", "RTA_REF2", "SEARCH_S014", "FORM1", "Check Number", 319255757, "S014", "DOCNAME", "T", "RTADOCDATE", "choque"),
    ("ضمان بنكي-إسم الشركة", "DOCNAME", "SEARCH_S018", "FORM1", "Bank Gurantee -Company", 320172749, "S018", "DOCNAME", "T*", "RTADOCDATE", None),
    ("ضمان بنكي-رقم الضمان", "RTAREFNUM", "SEARCH_S018", "FORM1", "Bank Gurantee Number", 320172889, "S018", "DOCNAME", "T*", "RTADOCDATE", None),
    ("فاتورة-رقم الفاتورة", "RTA_TEXT1", "SEARCH_S013", "FORM1", "Invoice Number", 319255830, "S013", "DOCNAME", "T", "RTADOCDATE", "Invoice"),
    ("فاتورة-رقم الوثيقة", "RTA_REF2", "SEARCH_S013", "FORM1", "Invoice-Document Number", 320169871, "S013", "DOCNAME", "T", "RTADOCDATE", "Invoice"),
    ("كود المشروع-نظام إدارة المشاريع", "RTAREFNUM", "SEARCH_S311", "FORM1", "Project Code-OPMS", 319365774, "S311,S312,S313,S314", "RTA_PROG_NAME", "T*", "CREATION_DATE", None),
    ("مالك مركبة", "NEW_OWNER_NAME", "SEARCH_S043", "FORM1", "Car Owner", 319246345, "S043", "NEW_OWNER_NAME", "*T*", "RENEWAL_DATE", None),
    ("مراسلات", "RTA_TEXT1", "SEARCH_G001", "FORM_NAME", "Correspondence", 318901278, "G001", "RTA_TEXT1", "*T*", "CREATION_DATE", None),
    ("مراسلات مشروع", "RTA_TEXT1", "SEARCH_G001", "FORM_NAME", "Project-Correspondence", 319365337, "G001", "RTA_TEXT1", "*T*", "CREATION_DATE", None),
    ("مرجع المناقصة", "RTAREFNUM", "SEARCH_G002", "FORM1", "Tender Ref", 466482105, "G002", "DOCNAME", "*T*", "RTADOCDATE", "Tender"),
    ("مركبة-رقم القاعدة Chassis Number", "CHASSIS_NUMBER", "SEARCH_S043", "FORM1", "Vehicle", 319080986, "S043", "TRANS_SERVICES", "T", "RENEWAL_DATE", None),
    ("مستندات الموقع الداخلي", "DOCNAME", "DEF_QBE", "FORM_NAME", "Intranet Files", 320687292, "S307", "DOCNAME", "T", "RTADOCDATE", None),
    ("مشروع-نظام إدارة المشاريع", "RTA_PROG_NAME", "SEARCH_S311", "FORM1", "Project", 319365066, "S011,S012,S013,S014", "RTA_PROG_NAME", "*T*", "CREATION_DATE", None),
    ("ملف مروري", "ID_NUMBER", "SEARCH_S045", "FORM1", "Traffic File No", 319006495, "S043,S045,S046,S084", "DOCNAME", "T", "CREATION_DATE", None),
    ("ملفات الموظفين", "NUM:RTA_REF2,STR:RTA_ARABIC_NAME", "SEARCH_S052", "FORM1", "Employees Files", 318995788, "S052", "DOCNAME", "T", "CREATION_DATE", None),
    ("موضوع المناقصة", "DOCNAME", "SEARCH_G002", "FORM1", "Tender Subject", 466484656, "G002", "DOCNAME", "*T*", "RTADOCDATE", "Tender"),
    ("نموذج الإجراءات", "RTAREFNUM", "SEARCH_G002", "FORM1", "Action Form Ref", 389339129, "G002", "RTAREFNUM", "T", "RTADOCDATE", None),
    ("وثائق الأنشطة التجارية", "DOCNAME", "DEF_QBE", "FORM_NAME", "Commercial Activity Refrences", 339533395, "DEF_PROF", "DOCNAME", "*T*", "RTADOCDATE", None),
    ("وصل", "RTA_REF2", "SEARCH_S012", "FORM_NAME", "Voucher Number(S012)", 322947409, "S012", "DOCNAME", "T", "RTADOCDATE", "Voucher"),
    # Additional entry visible in production screenshot
    ("وثيقة عامة", "DOCNAME", "SEARCH_S355", "FORM_NAME", "General Document Subject - S355", 349207429, "S355", "DOCNAME", "*T*", "RTADOCDATE", "Vregistration"),
]


async def create_and_populate():
    conn = await get_async_connection()
    if not conn:
        print("ERROR: Failed to connect to database.")
        return

    try:
        async with conn.cursor() as cursor:
            # Check if table already exists
            table_exists = False
            try:
                await cursor.execute("SELECT 1 FROM LKP_MAIN_FIELDS FETCH FIRST 1 ROWS ONLY")
                table_exists = True
                print("[INFO] LKP_MAIN_FIELDS table already exists.")
                await cursor.execute("SELECT COUNT(*) FROM LKP_MAIN_FIELDS")
                count_row = await cursor.fetchone()
                print(f"[INFO] Current row count: {count_row[0]}")
                
                response = input("Table already exists. Drop and recreate? (y/n): ").strip().lower()
                if response != 'y':
                    print("Aborted.")
                    return
                
                await cursor.execute("DROP TABLE LKP_MAIN_FIELDS")
                print("[OK] Dropped existing table.")
            except Exception:
                if not table_exists:
                    print("[INFO] Table does not exist, will create it.")

            # Create the table - matches production schema
            create_sql = """
                CREATE TABLE LKP_MAIN_FIELDS (
                    SYSTEM_ID       NUMBER,
                    DISABLED        VARCHAR2(1) DEFAULT 'N',
                    TARGET_D        DATE,
                    LAST_UPDATE     DATE,
                    TYPE_NAME       VARCHAR2(500),
                    TYPE_NAME_EN    VARCHAR2(500),
                    FORM            VARCHAR2(200),
                    FIELD_NAME      VARCHAR2(500),
                    FORM_SRCH_FIELD VARCHAR2(200),
                    SEARCH_FORM     VARCHAR2(500),
                    EXACT           VARCHAR2(20),
                    DISPLAY         VARCHAR2(200),
                    DATE_FIELD      VARCHAR2(100),
                    KEYID           VARCHAR2(200)
                )
            """
            await cursor.execute(create_sql)
            print("[OK] Created LKP_MAIN_FIELDS table.")

            # Insert all rows
            insert_sql = """
                INSERT INTO LKP_MAIN_FIELDS 
                    (TYPE_NAME, FIELD_NAME, FORM, FORM_SRCH_FIELD, TYPE_NAME_EN, 
                     SYSTEM_ID, SEARCH_FORM, DISPLAY, EXACT, DATE_FIELD, KEYID, DISABLED)
                VALUES 
                    (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, 'N')
            """
            
            for row in LEGACY_SEARCH_TYPES:
                await cursor.execute(insert_sql, row)
            
            await conn.commit()
            print(f"[OK] Inserted {len(LEGACY_SEARCH_TYPES)} search type rows.")

            # Verify
            await cursor.execute("SELECT COUNT(*) FROM LKP_MAIN_FIELDS WHERE DISABLED <> 'Y'")
            count_row = await cursor.fetchone()
            print(f"[OK] Verification: {count_row[0]} active search types in table.")

            # Show a sample
            await cursor.execute("SELECT TYPE_NAME, TYPE_NAME_EN, FORM FROM LKP_MAIN_FIELDS FETCH FIRST 5 ROWS ONLY")
            sample_rows = await cursor.fetchall()
            print("\n--- Sample rows ---")
            for r in sample_rows:
                print(f"  {r[0]}  ({r[1]})  [{r[2]}]")

            print("\n--- SUCCESS ---")
            print("LKP_MAIN_FIELDS table created and populated with production data.")
            print("Restart the EDMS API and test /api/researcher/types")

    except Exception as e:
        print(f"ERROR: {e}")
        try:
            await conn.rollback()
        except:
            pass
    finally:
        await conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(create_and_populate())
