
طريقة العمل بسرعة:
1) ضع ملفات التقارير اليومية داخل مجلد واحد (نفس الصيَغ التي عندك).
2) أنشئ ملف staff_master.csv من التمبلِت المرفق وأضِف كل الموظفين (staff_id هو الرقم الموجود قبل الاسم أو في "Unknown 2792").
3) شغّل:
   python etl_sales_dashboard.py --in-dir "<المجلد>" --out-dir "<المجلد>/dashboard_data"
4) انسخ index.html ومجلد dashboard_data إلى أي سيرفر/Netlify/GitHub Pages وافتح الصفحة.

ملاحظات:
- لو كانت الملفات بصيغة .xls تحتاج xlrd==1.2.0
- لو .xlsx استخدم openpyxl
- ملف facts.json هو المصدر الرئيسي للواجهة.
