import random
from math import pi

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import CSS4_COLORS

colors_set1 = cm.get_cmap('Set1', len(CSS4_COLORS))
color_names = [colors_set1(i) for i in range(len(CSS4_COLORS))]

def generate_color_string(length):
    color_list = []
    while len(color_list) < length:
        color_list.append(random.choice(color_names))
    return color_list


def plot_spider_client(data, title, color, client, year, name_saved_file):
    data_client = data[(data['CLIENT'] == client) & (data['YEAR'] == year)]

    # number of variable
    categories = list(data_client.columns)[2:-1]
    categories_number = len(categories)

    # Angle of each axis in the plot
    angles = [category / float(categories_number) * 2 * pi for category in range(categories_number)]
    angles += angles[:1]

    # Initialise the spider plot
    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))

    # make the first axis to be on top:
    ax.set_theta_offset(pi / 2)
    ax.set_theta_direction(-1)

    # Ind1
    values = data_client.iloc[0, 2:-1].values.flatten().tolist()
    values += values[:1]

    # Draw one axe per variable + add labels labels yet
    plt.xticks(angles[:-1], categories, fontweight='bold', fontsize=15)
    plt.xticks(angles[:-1], categories, color='grey', size=8)

    # Draw ylabels
    ax.set_rlabel_position(0)
    plt.yticks(color="grey", size=7)
    plt.ylim(0, max(values) * 1.2)

    ax.plot(angles, values, color=color, linewidth=2, linestyle='solid')
    ax.fill(angles, values, color=color, alpha=0.4)

    # Annotare i valori alla punta di ogni sezione
    for i, value in enumerate(values[:-1]):
        angle_rad = angles[i]
        ax.text(angle_rad, value + max(values) * 0.07, str(value), horizontalalignment='center', size=12, color='black',
                weight='semibold')

    # Add a title
    plt.title(title, size=18, color='black', fontweight='bold', y=1.1)
    plt.savefig("images/" + name_saved_file + ".png")  # Salvataggio del grafico come immagine PNG
    plt.show()
