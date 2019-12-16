import os
from luigi import Task
from fpdf import FPDF
import glob
from datetime import date


class PdfReport(Task):
    """When called, this script will look in the images folder,
        and create a pdf-report of all the images in this folder.
        Hence, you need to create some graphs based on your need
        for this to actually create anything"""

    def return_env(self, value):
        # Fix required for Travis CI
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value

    def run(self, *args, **options):
        files = glob.glob(self.return_env("local_location") + "images/*")
        pdf = FPDF(orientation="L")
        pdf.add_page()
        pdf.set_font("Arial", size=32)
        pdf.cell(270, 150, txt="Oanda Reports", ln=1, align="C")
        pdf.set_font("Arial", size=16)
        pdf.cell(270, 10, txt="Report created {}".format(date.today()), ln=1, align="C")
        for i in files:
            pdf.add_page()
            pdf.image(i, x=5, y=5, h=200)

        if not os.path.exists(self.return_env("local_location") + "report/"):
            os.makedirs(self.return_env("local_location") + "report/")

        pdf.output(self.return_env("local_location") + "report/" + "OandaReport.pdf")
