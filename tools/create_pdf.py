import os
from luigi import Task
from fpdf import FPDF
import glob
from datetime import date

class PdfReport(Task):
    help = "Will create a pdf-document of the images created earlier"

    def run(self, *args, **options):
        files = glob.glob(os.getenv("local_location") + "images/*")
        pdf = FPDF(orientation='L')
        pdf.add_page()
        pdf.set_font("Arial", size=32)
        pdf.cell(270, 150, txt="Oanda Reports", ln=1, align="C")
        pdf.set_font("Arial", size=16)
        pdf.cell(270, 10, txt="Report created {}".format(date.today()), ln=1, align="C")
        for i in files:
            pdf.add_page()
            pdf.image(i, x=5, y=5, h=200)

        if not os.path.exists(os.getenv("local_location") + "report/"):
            os.makedirs(os.getenv("local_location") + "report/")

        pdf.output(os.getenv("local_location") + "report/" + "OandaReport.pdf")