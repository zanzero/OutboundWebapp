import time
import pandas as pd
import sqlite3
from flask_paginate import Pagination, get_page_args
from flask import Flask, render_template, request, redirect
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import io
from outbound_engine import OutboundNG

app = Flask(__name__)


@app.route('/')
def home():
    connection = sqlite3.connect(OutboundNG.db_name)
    cursor = connection.cursor()
    cursor.execute('select count(*) from outbound_called')
    total = cursor.fetchone()[0]

    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')

    query = f"SELECT * FROM outbound_called ORDER BY id DESC LIMIT {per_page} OFFSET {offset}"
    cursor.execute(query)

    rows = cursor.fetchall()
    new_rows_list = list()
    for row in rows:
        new_rows = list(row)
        new_rows[4] = new_rows[4][:4] + '***' + new_rows[4][7:]
        new_rows_list.append(new_rows)

    cursor.close()
    connection.close()

    pagination = Pagination(page=page,
                            per_page=per_page,
                            total=total,
                            css_framework='bootstrap4')

    return render_template('index.html', rows=new_rows_list,
                           page=page,
                           per_page=per_page,
                           pagination=pagination, )


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    OutboundNG.list_name = file.filename.split(".")[0]
    if file.filename.split(".")[-1] != "csv":
        return "Invalid file type, please upload a CSV"

    file_contents = io.BytesIO(file.read())
    OutboundNG.df = pd.read_csv(file_contents)
    return redirect("/showlist")


@app.route('/showlist', methods=['GET'])
def showlist():
    return render_template('showlist.html', df=OutboundNG.df.to_html(classes='table table-stripped table-sm',
                                                                     border=2), list_name=OutboundNG.list_name)


@app.route('/runlist', methods=['POST'])
def runlist():
    OutboundNG.run_list(df=OutboundNG.df, list_name=OutboundNG.list_name)
    return redirect('/')


@app.route('/reuse', methods=['GET', 'POST'])
def reuse():
    connection = sqlite3.connect(OutboundNG.db_name)
    cursor = connection.cursor()

    if request.method == "POST":
        cursor.execute(f"SELECT * FROM outbound_called WHERE apiexec == 'N' \
        AND number_of_calls <= {OutboundNG.option_number_of_reuse}")

        reuse_list = cursor.fetchall()
        OutboundNG.re_use(reuse_list)

        cursor.close()
        connection.close()
        return redirect('/')

    cursor.execute(
        f"select count(*) from outbound_called WHERE apiexec == 'N' AND number_of_calls <= {OutboundNG.option_number_of_reuse}")
    total = cursor.fetchone()[0]

    page, per_page, offset = get_page_args(page_parameter='page',
                                           per_page_parameter='per_page')

    query = f"SELECT * FROM outbound_called WHERE apiexec == 'N' AND number_of_calls <= {OutboundNG.option_number_of_reuse} \
            ORDER BY id DESC LIMIT {per_page} OFFSET {offset}"

    cursor.execute(query)

    rows = cursor.fetchall()
    reuse_list = list()
    for row in rows:
        new_rows = list(row)
        new_rows[4] = new_rows[4][:4] + '***' + new_rows[4][7:]
        reuse_list.append(new_rows)

    cursor.close()
    connection.close()

    pagination = Pagination(page=page,
                            per_page=per_page,
                            total=total,
                            css_framework='bootstrap4')

    return render_template('reuse.html', rows=reuse_list,
                           page=page,
                           per_page=per_page,
                           pagination=pagination, )


@app.route('/option', methods=['GET', 'POST'])
def option():
    options = ["Run", "Pause"]

    if request.method == 'POST':
        OutboundNG.option_number_of_reuse = request.form["calls"]
        OutboundNG.option_repeat_x_minutes = request.form["repeat"]
        OutboundNG.option_repeat_yes_or_no = request.form["selected_option"]
        if request.form["selected_option"] == "Pause":
            pause_task()
        else:
            restart_task()

    return render_template('option.html', option_number_of_reuse=OutboundNG.option_number_of_reuse,
                           option_repeat_x_minutes=OutboundNG.option_repeat_x_minutes, options=options,
                           selected_option=OutboundNG.option_repeat_yes_or_no)


def run_task():
    if OutboundNG.option_repeat_yes_or_no == "Run":
        print(f"Run Task {time.ctime()}")
        connection = sqlite3.connect(OutboundNG.db_name)
        cursor = connection.cursor()

        cursor.execute(f"SELECT * FROM outbound_called WHERE apiexec == 'N' \
                AND number_of_calls <= {OutboundNG.option_number_of_reuse}")

        reuse_list = cursor.fetchall()
        OutboundNG.re_use(reuse_list)

        cursor.close()
        connection.close()
        print(f"Task Completed {time.ctime()}")


@app.route('/pause_task', methods=['POST'])
def pause_task():
    scheduler.pause()
    print("Task paused")


@app.route('/restart_task', methods=['POST'])
def restart_task():
    new_trigger = IntervalTrigger(minutes=int(OutboundNG.option_repeat_x_minutes))
    scheduler.reschedule_job(job.id, trigger=new_trigger)
    print(f"Task restarted every {OutboundNG.option_repeat_x_minutes} Mins")


scheduler = BackgroundScheduler()
job = scheduler.add_job(run_task, 'interval', minutes=int(OutboundNG.option_repeat_x_minutes))
scheduler.start()
print(f"Task Started every {OutboundNG.option_repeat_x_minutes} Mins")


@app.route('/doc')
def doc():
    return render_template('doc.html')

#Hi
if __name__ == '__main__':
    app.run(debug=False, port=5000)
