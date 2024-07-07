import DataBaseFunction
import GraphConstructor
import PdfConstructor

if __name__ == '__main__':
    dc = DataBaseFunction.execute_query(DataBaseFunction.find_clients())
    df = DataBaseFunction.execute_query(DataBaseFunction.uploaded_from_stato())
    list = PdfConstructor.extraction_images()
    print(df, dc)
    clients = dc.values.flatten()
    colors = GraphConstructor.generate_color_string(len(clients))
    year = 2024
    for i in range(len(clients)):
        GraphConstructor.plot_spider_client(df, clients[i], colors[i], clients[i], year, clients[i] + '_' + str(year))
#   PdfConstructor.generate_pdf_with_chart()
