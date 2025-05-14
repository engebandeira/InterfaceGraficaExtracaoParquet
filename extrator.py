import os
import flet as ft
import pandas as pd
import unicodedata
import asyncio
import csv
import re
import psutil
import pyarrow.parquet as pq
from datetime import datetime


def list_date_folders(base_dir, start_date, end_date):
    ''''
    Função para listar pastas que correspondem ao padrão ano-mês dentro de um intervalo de datas
    
    base_dir: Diretorio das pastas contendo os arquivos parquet
    start_date: ano-mes de inicio da extração
    end_date: ano-mes de final da extração
    '''
    
    start_date =  datetime.strptime(start_date, '%Y-%m')
    end_date =  datetime.strptime(end_date, '%Y-%m')

    date_pattern = re.compile(r'^\d{4}(-\d{2})?$')
    folders = [f for f in os.listdir(base_dir) if date_pattern.match(f)]
    valid_folders = []
    
    for folder in folders:
        folder_date = datetime.strptime(folder, '%Y-%m')

        if start_date <= folder_date <= end_date:
            valid_folders.append(folder)

    return valid_folders

async def read_parquet_files(base_dir, date_folders, desired_columns, progress_bar):
    all_dfs = []
    total_files = 0
    for folder in date_folders:
        folder_path = os.path.join(base_dir, folder)
        total_files += len([f for f in os.listdir(folder_path) if f.endswith('.parquet')])

    processed_files = 0
    for folder in date_folders:
        folder_path = os.path.join(base_dir, folder)
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.parquet'):
                file_path = os.path.join(folder_path, file_name)
                parquet_file = pq.ParquetFile(file_path)
                available_columns = parquet_file.schema.names
                valid_columns = [col for col in desired_columns if col in available_columns]

                df = pd.read_parquet(file_path, columns=valid_columns, engine='pyarrow')
                df = df.drop_duplicates()
                all_dfs.append(df)

                processed_files += 1
                progress = processed_files / total_files
                progress_bar.value = progress
                progress_bar.update()
                await asyncio.sleep(0.1)  # Simulate async work

    final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame(columns=desired_columns)
    return final_df

async def main(page: ft.Page):

    page.theme_mode = ft.ThemeMode.DARK 
    page.vertical_alignment = ft.MainAxisAlignment.SPACE_AROUND
    page.window_width = 1280
    page.window_height = 850
    page.adaptive = True
    page.scroll = "adaptive"  # Enable adaptive scroll


    caminho_p1 = r"\\192.168.0.4\Credito_Fraudes\02. Crédito\10_Transformacoes_Bases\relatoriosCrivo\P1\2_bases_tratadas"
    caminho_p2 = r"\\192.168.0.4\Credito_Fraudes\02. Crédito\10_Transformacoes_Bases\relatoriosCrivo\P234\2_bases_tratadas"
    
    prefixos = ['CAMPO',
                      'COLETA',
                      'DECISAO',
                      'GLOBAL',
                      'PARAMETRO_SAIDA_CRITERIO',
                      'VARIAVEL_CRITERIO',
                      'VARIAVEL_DRIVER']

    caminho_text = ft.Text()
    colunas_filtro = ''

    dados = []
    checkboxes = []

    def dropdown_changed(e):
        nonlocal dados, checkboxes, colunas_filtro

        # Alterar a cor do texto com base na opção selecionada
        if e.control.value == "Extração P1":
            caminho_text.value = caminho_p1
            colunas_filtro = 'colunas_p1.txt'
        elif e.control.value == "Extração P2/3/4":
            caminho_text.value = caminho_p2
            colunas_filtro = 'colunas_p2.txt.'
        
        with open(colunas_filtro, 'r') as file:
            reader = csv.reader(file, delimiter=';') 
            for row in reader:
                if any(row):  
                    dados.extend(row)

        dados = sorted(dados)
    
        checkboxes = [ft.Checkbox(label=item) for item in dados]
        selecao_ps.disabled = True
        list_view.controls = checkboxes  # Update the ListView with new checkboxes
        list_view.update()
        page.update()


    selecao_ps = ft.Dropdown(
        width=300,
        label='Extração',
        hint_text='Selecione a "P" para extração',
        options=[
            ft.dropdown.Option("Extração P1"),
            ft.dropdown.Option("Extração P2/3/4"),
        ],
        autofocus=True,
        on_change=dropdown_changed
    )

    def verificar_formato_data(data):
        # Expressão regular para verificar o formato YYYY-MM e se os dois últimos dígitos estão entre 01 e 12
        regex = r"^(?:\d{4})-(?:0[1-9]|1[0-2])$"
        return bool(re.match(regex, data))

    def verificar_datas():
        start_date = start_date_input.value
        end_date = end_date_input.value
        valid_start_date = verificar_formato_data(start_date)
        valid_end_date = verificar_formato_data(end_date)
        dates_valid = False


        if valid_start_date and valid_end_date:
            if end_date >= start_date:
                start_date_input.error_text = ""
                end_date_input.error_text = ""
                dates_valid = True
                b_submit_dates.disabled = False
            else:
                b_submit_dates.disabled = True
                end_date_input.error_text = "Data de Fim deve ser maior ou igual à Data de Início"
        else:
            b_submit_dates.disabled = True
            if not valid_start_date:
                start_date_input.error_text = "Insira a Data no formato YYYY-MM"
            if not valid_end_date:
                end_date_input.error_text = "Insira a Data no formato YYYY-MM"

        page.update()
        return dates_valid

    start_date_input = ft.TextField(label="Data de Início (YYYY-MM):", width=300, on_change=lambda e: verificar_datas())
    end_date_input = ft.TextField(label="Data de Fim (YYYY-MM):", width=300, on_change=lambda e: verificar_datas())

    start_date_text = ft.Text()
    end_date_text = ft.Text()

    def submit_dates(e):
        if verificar_datas():
            start_date_text.value = start_date_input.value
            end_date_text.value = end_date_input.value
            colunas.disabled = False
        page.update()

    b_submit_dates = ft.ElevatedButton('Escolher Datas', on_click=submit_dates,disabled=True)

    # Função para normalizar e remover acentos
    def normalize(text):
        return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn').lower()

    # Estado do prefixo selecionado
    selected_prefix = None

    def update_visibility():
        for checkbox in checkboxes:
            checkbox.visible = (selected_prefix is None or checkbox.label.startswith(selected_prefix))
        list_view.scroll_to(0)
        list_view.update()

    # Função para filtrar os checkboxes com base no prefixo
    def filter_by_prefix(prefix):
        nonlocal selected_prefix
        selected_prefix = prefix
        search_box.value = None
        list_view.scroll_to(0)
        update_visibility()

    # Função para filtrar os checkboxes com base na entrada de pesquisa
    def filter_checkboxes(text):
        normalized_text = normalize(text)
        pattern = re.compile(re.escape(normalized_text), re.IGNORECASE)  # Cria um padrão de busca insensível a maiúsculas e acentos
        # Atualiza a visibilidade com base no prefixo selecionado e na busca
        for checkbox in checkboxes:
            checkbox.visible = (selected_prefix is None or checkbox.label.startswith(selected_prefix)) and bool(pattern.search(normalize(checkbox.label)))
        # Redefine a rolagem para o topo da ListView
        list_view.scroll_to(0)
        # Atualiza o layout da ListView para se adaptar ao tamanho da nova lista
        list_view.update()
        page.update()

    # Função para mostrar todos os itens
    def show_all_items():
        nonlocal selected_prefix
        selected_prefix = None  # Reseta o filtro de prefixo
        for checkbox in checkboxes:
            checkbox.visible = True  # Torna todos os checkboxes visíveis
        search_box.value = None
        list_view.scroll_to(0)  # Redefine a rolagem para o topo da ListView
        list_view.update()  # Atualiza o layout da ListView
        page.update()  # Atualiza a página

    # Criar botões de pré-filtragem
    prefix_buttons = [ft.ElevatedButton(text=prefix, on_click=lambda e, p=prefix: filter_by_prefix(p)) for prefix in prefixos]

    # Botão para mostrar todos os itens
    show_all_button = ft.ElevatedButton(text="Mostrar Todas", on_click=lambda e: show_all_items())

    # Row para alinhar os botões de pré-filtragem
    buttons_row = ft.Row(controls=[show_all_button, *prefix_buttons])

    # Campo de texto para entrada de pesquisa
    search_box = ft.TextField(hint_text="Pesquisar Colunas", height=50, on_change=lambda e: filter_checkboxes(e.control.value))

    # Criar um ListView para os checkboxes
    list_view = ft.ListView(
        controls=checkboxes,
        expand=True,
        height=430,
        spacing=10,
        padding=20,
        cache_extent=1500
    )

    container_list = ft.Container(content=list_view, expand=True, alignment=ft.MainAxisAlignment.CENTER)
    colunas = ft.Column(controls=[search_box, buttons_row, container_list], spacing=10)

    # Função para selecionar todos os checkboxes visíveis
    def select_all_visible(e):
        for checkbox in checkboxes:
            if checkbox.visible:
                checkbox.value = True
        page.update()

    # Função para limpar todas as seleções dos checkboxes visíveis
    def clear_all_selections(e):
        for checkbox in checkboxes:
            if checkbox.visible:
                checkbox.value = False
        page.update()

    def show_all_true(e):
        list_view.scroll_to(0)
        list_view.update()
        for checkbox in checkboxes:
            checkbox.visible = checkbox.value
        page.update()

    select_all_button = ft.ElevatedButton(text="Selecionar Todos", on_click=select_all_visible)
    clear_all_button = ft.ElevatedButton(text="Limpar Seleções", on_click=clear_all_selections)
    show_all_true_button = ft.ElevatedButton(text="Mostrar Selecionadas", on_click=show_all_true)

    def select_from_file(file_content):
        # Separar os nomes dos itens pelo delimitador ';'
        item_names = file_content.split(';')
        # Normalizar os nomes dos itens para garantir a correspondência
        normalized_item_names = [normalize(name.strip()) for name in item_names]
        # Selecionar os checkboxes correspondentes
        for checkbox in checkboxes:
            checkbox.value = normalize(checkbox.label) in normalized_item_names
        page.update()

    def on_dialog_result(e: ft.FilePickerResultEvent):
        if e.files:  # Check if any files are selected
            caminho_arquivo = e.files[0].path
            print(caminho_arquivo)
            if caminho_arquivo:
                with open(caminho_arquivo, 'r') as file:
                    file_content = file.read()
                select_from_file(file_content)

    colunas_costumizadas = ft.FilePicker(on_result=on_dialog_result)
    page.overlay.append(colunas_costumizadas)
    page.update()

    botao_selecao = ft.ElevatedButton(
        text='Filtrar Colunas Customizado',
        icon=ft.icons.UPLOAD_FILE,
        tooltip='Selecione um arquivo de texto com as colunas separadas por ";" \n Exemplo: campo_1;campo_2;...',
        on_click=lambda _: colunas_costumizadas.pick_files(allowed_extensions=['txt'])
    )

    final_selection = []
    async def finalizar_selecao(e):
        final_selection.clear()
        for checkbox in checkboxes:
            if checkbox.value:
                final_selection.append(checkbox.label)
        
        pastas = list_date_folders(caminho_text.value, start_date_input.value, end_date_input.value)
        
        layout_anterior.visible = False

        def mysavefile(e: ft.FilePickerResultEvent):
            save_location = e.path
            if save_location:
                df.to_csv(save_location, index=False,sep=';',encoding='iso-8859-1')

        saveme = ft.FilePicker(on_result=mysavefile)
        page.overlay.append(saveme)

        extracao = ft.Text('PROGRESSO DA EXTRAÇÃO:')
        progress_bar = ft.ProgressBar(width=500, height=35, value=0)
        progress_bar_row = ft.Row(controls=[progress_bar])
        saveme_button = ft.ElevatedButton('Salvar base',on_click= lambda _: saveme.save_file(file_name='extracao.csv'),disabled=True)

        layout_final.controls.append(extracao)
        layout_final.controls.append(progress_bar_row)
        layout_final.controls.append(saveme_button)

        layout_final.alignment = ft.MainAxisAlignment.CENTER
        page.window_width = 540
        page.window_height = 200

        page.update()
  
        df = await read_parquet_files(caminho_text.value, pastas, final_selection, progress_bar)
        saveme_button.disabled = False
        page.update()
    
        print(df)

    botao_finalizar = ft.ElevatedButton(
        text='Iniciar Extração',
        icon=ft.icons.CHECK_CIRCLE,
        on_click=finalizar_selecao
    )

    selecao_row = ft.Row(controls=[select_all_button, clear_all_button, show_all_true_button, botao_selecao, botao_finalizar])
    container_list = ft.Container(content=list_view)
    colunas = ft.Column(controls=[search_box, buttons_row, container_list, selecao_row], spacing=10, expand=True, disabled=True)

    row_dates = ft.Row(controls=[start_date_input, end_date_input, b_submit_dates])

    layout_anterior =  ft.ListView( 
        spacing=20,
        padding=25,
        controls=[selecao_ps, row_dates, colunas])
    
    layout_final =ft.Column(controls=[ layout_anterior ],alignment=ft.MainAxisAlignment.CENTER,spacing=20)

    page.add(layout_final)

if __name__ == '__main__':
    ft.app(target=main)