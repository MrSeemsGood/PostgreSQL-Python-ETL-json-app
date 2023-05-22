import json
import sys
import os
import psycopg2
from psycopg2 import extras
from PyQt5.QtCore import QEvent
from PyQt5.QtWidgets import (
    QLabel, QApplication, QMainWindow, QWidget, QTableWidget, QScrollArea, QDialog,
    QMessageBox, QLineEdit, QPushButton, QTableWidgetItem, QFileDialog, QGridLayout, QFrame
)


def deconstruct_dict(json_dict:dict):
    '''Deconstruct a dictionary into a list of tuples
    representable for the database.'''
    deconstructed = list()
    for subdict in json_dict['data']:
        price_change_id = subdict['id']
        for price_change in subdict['price_change']:
            deconstructed.append((price_change_id, price_change['price'], price_change['eff_from']))

    return deconstructed


def write_to_db(data, connection, cursor):
    '''Write data to the database.'''
    cursor.execute('SELECT CURRENT_TIMESTAMP(0);')
    start = cursor.fetchone()

    try:
        extras.execute_values(
            cursor,
            "INSERT INTO prices_schema.prices(id, price, eff_from) VALUES %s",
            data
            )
        connection.commit()
        cursor.execute('SELECT CURRENT_TIMESTAMP(0);')
        end = cursor.fetchone()
        return start, end
    except:
        return None, None


def fetch_from_db(cursor):
    '''Fetch results from the database.'''
    cursor.execute("""
        SELECT * FROM prices_schema.prices;
        """)

    return cursor.fetchall()


def functionality(filepath:str):
    '''The "everything" function called when the app accepts a .json input file.'''
    # read .json file
    with open(filepath, 'r', encoding='utf-8') as file:
        loaded:dict = json.load(file)

    # connect to db and create a cursor
    prices_conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='prices',
        user='postgres',
        password='postgredb1'
    )
    prices_cur = prices_conn.cursor()

    process_id = loaded['process_id']
    prices_cur.execute(
        'SELECT * FROM prices_schema.process_journal WHERE process_id = %s',
        [process_id]
        )
    if len(prices_cur.fetchall()) > 0:
        parserAppWindow.centralWidget().findChild(QLabel).setText('Failed')
        QMessageBox.information(
            None,
            'process_journal entry failed',
            'This process id already exists in: process_journal.process_id'
            )
        prices_cur.close()
        prices_conn.close()
        return

    # deconstruct loaded dictionary into values
    deconstructed = deconstruct_dict(loaded)

    # write to db
    parserAppWindow.centralWidget().findChild(QLabel).setText('Processing...')
    parserApp.processEvents()
    start_ts_time, end_ts_time = write_to_db(deconstructed, prices_conn, prices_cur)

    if start_ts_time is None:
        parserAppWindow.centralWidget().findChild(QLabel).setText('Failed')
        prices_cur.close()
        prices_conn.close()
        return

    # fetch from db
    parserAppWindow.centralWidget().findChild(QLabel).setText('Fetching...')
    parserApp.processEvents()
    result_table = fetch_from_db(prices_cur)
    parserAppWindow.centralWidget().findChild(QLabel).setText('Success')

    # write to journal
    extras.execute_values(
        prices_cur,
        """
            INSERT INTO prices_schema.process_journal(process_id, file_name, start_ts, end_ts)
            VALUES %s
        """,
        [(process_id, os.path.basename(filepath), start_ts_time, end_ts_time)]
        )
    prices_conn.commit()

    # update table widget
    parserAppWindow.update_table_widget(result_table)

    # close connections
    prices_cur.close()
    prices_conn.close()


def is_json_specified(file_path:str):
    '''Check if given path allows to retrieve a .json file.'''
    check = file_path.endswith('.json')
    if not check:
        QMessageBox.information(
            None,
            'Wrong file format',
            'Only .json files can be accepted for parsing'
            )
        return False

    return True


class AcceptDropsFrame(QFrame):
    '''Custom inherited class of QFrame that allows for custom drag&drop events.'''
    def dragEnterEvent(self, event:QEvent):
        if event.mimeData().hasUrls():
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event:QEvent):
        file = event.mimeData().urls()[0].toLocalFile()
        if is_json_specified(file):
            self.parent().findChild(QLineEdit).setText(file)
            functionality(file)


class ParserAppWindow(QMainWindow):
    '''Main class for the app's main window.'''
    def __init__(self) -> None:
        super().__init__()

        self.appearance()
        self.place_widgets()
        self.show()

    def appearance(self):
        '''Set window's appearance.'''
        self.setFixedSize(450, 550)
        self.setWindowTitle('ETL .json parser')

    def place_widgets(self):
        '''Place widgets.'''
        central_widget = QWidget()
        grid = QGridLayout()

        # row 1
        grid.addWidget(QLabel('Select .json file:'), 1, 1, 1, 1)
        path_edit = QLineEdit(parent=self.centralWidget())
        path_edit.setDisabled(True)
        grid.addWidget(path_edit, 1, 2, 1, 1)
        select_button = QPushButton('Select')
        select_button.clicked.connect(self.update_path_edit)
        grid.addWidget(select_button, 1, 3, 1, 1)

        # row 2
        grid.addWidget(QLabel('OR drop it here:'), 2, 1, 1, 1)

        # row 3
        accept_drops_frame = AcceptDropsFrame(parent=self.centralWidget())
        accept_drops_frame.setAcceptDrops(True)
        accept_drops_frame.setStyleSheet('QFrame { border-image : url("icons/dragdrop.png");}')
        grid.addWidget(accept_drops_frame, 3, 1, 1, 3)

        # row 4
        grid.addWidget(QLabel('Results:'), 4, 1, 1, 1)
        state_label = QLabel(text='', parent=central_widget)
        grid.addWidget(state_label, 4, 2, 1, 1)

        # row 5
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        table = QTableWidget(3, 4, scroll)
        table.setHorizontalHeaderLabels(('id', 'price', 'eff_from', 'eff_to'))

        scroll.setWidget(table)
        grid.addWidget(scroll, 5, 1, 1, 3)

        grid.setRowStretch(1, 0)
        grid.setRowStretch(2, 0)
        grid.setRowStretch(3, 1)
        grid.setRowStretch(4, 0)
        grid.setRowStretch(5, 1)

        central_widget.setLayout(grid)
        self.setCentralWidget(central_widget)

    def update_table_widget(self, content:list[tuple]):
        '''Update QTableWidget with fetched data.'''
        table_widget:QTableWidget = self.centralWidget().findChild(QScrollArea).widget()
        table_widget.clearContents()
        table_widget.setRowCount(len(content))
        for i, content_row in enumerate(content):
            for j, value in enumerate(content_row):
                item = QTableWidgetItem(str(value))
                table_widget.setItem(i, j, item)

    def update_path_edit(self):
        '''Process .json file specified by the "Select" button.'''
        dialog = QFileDialog(
            self.centralWidget(),
            'Select a file',
            os.path.abspath(__file__),
            'JSON file (*.json)'
            )
        if dialog.exec_() == QDialog.Accepted:
            file = dialog.selectedFiles()[0]
            if is_json_specified(file):
                self.centralWidget().findChild(QLineEdit).setText(file)
                functionality(file)

parserApp = QApplication(sys.argv)

parserAppWindow = ParserAppWindow()
parserApp.exec_()
