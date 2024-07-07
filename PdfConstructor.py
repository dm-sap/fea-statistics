from datetime import datetime

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import os

from reportlab.platypus import SimpleDocTemplate


# Funzione per aggiungere un'immagine al PDF
def add_image_to_pdf(pdf_canvas, image_path, x, y, width, height):
    pdf_canvas.drawImage(image_path, x, y, width, height, preserveAspectRatio=True, anchor='c')


def extraction_images():
    folder_path = 'images'
    list_images = []
    if os.path.isdir(folder_path):
        for item in os.listdir(folder_path):
            list_images.append(item)
        return list_images
    else:
        print(f"Il percorso '{folder_path}' non è una cartella valida.")


# Funzione principale per generare il PDF con il grafico
def generate_pdf_with_chart():
    # Creazione di un nuovo documento PDF
    pdf_filename = 'REPORT.pdf'
    pdf_canvas = canvas.Canvas(pdf_filename, pagesize=letter)

    # Aggiunta del titolo al PDF
    pdf_canvas.setFont('Helvetica-Bold', 16)
    pdf_canvas.drawString(100, 900, 'Report Mensile')

    pdf_canvas.setFont('Helvetica', 12)
    pdf_canvas.drawString(110, 50,
                          'Documento di monitoraggio per le statistiche della FEA.')

    x = 120
    delta = 300
    for image in extraction_images():
        client_name = image[:image.index('_')]
        client_year = image[image.index('_'):image.index('.')]
        pdf_canvas.drawString(x, 50, client_name)
        x += 10
        chart_image_path = 'images/' + image
        add_image_to_pdf(pdf_canvas, chart_image_path, x, y=600, width=400, height=delta)
        pdf_canvas.setFont('Helvetica', 12)
        x += delta
        pdf_canvas.drawString(x, 500, "Grafico dei documenti caricati da " + client_name + "nell'anno " + client_year)

    pdf_canvas.save()

