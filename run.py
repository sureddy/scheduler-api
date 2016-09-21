from scheduler import app, app_init
app_init(app)

app.run(host='0.0.0.0', debug=True)
