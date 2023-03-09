import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import dash
from dash import dcc
from dash import html
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from dash.dependencies import Input, Output

cnxn = pyodbc.connect(
    'Driver={ODBC Driver 17 for SQL Server}; Server=CMDS-SQL02.CMDS.local; Database=CDB; Trusted_Connection=yes;')

query = """select concat('investor_turnover_', vt1.turnover) as profile_var
	, client_1 as client_curr
	, client_2 as client_old
	, client_europe
FROM 
(
	SELECT 
	'AAR202207.1' AS project_code,
	rk.turnover, 
	COUNT(distinct advisor_ent_id) AS client_1
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].[ref].turnover_key rk ON rk.turnover_key = or1.turnover_key
	WHERE client_code = 'AAR' AND project_code = 'AAR202207.1'
	group by rk.turnover
) vt1

JOIN (
	SELECT 
	'AAR202002.2' AS project_code,
	turnover, 
	COUNT(distinct advisor_ent_id) AS client_2
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].[ref].turnover_key rk ON rk.turnover_key = or1.turnover_key
	WHERE client_code = 'AAR' AND project_code = 'AAR202002.2'
	group by rk.turnover
) vt2

on vt2.turnover = vt1.turnover

left join (

select count(distinct vt.advisor_ent_id) as client_europe
		, turnover
from (
	SELECT max(report_yr) as max_year
		, advisor_ent_id
		, rk.turnover
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].[ref].turnover_key rk ON rk.turnover_key = or1.turnover_key
	JOIN [CDB].ref.country_key kc on or1.investor_country_key = kc.country_key
	JOIN [CDB].ref.region_key rc on rc.region_key = kc.region_key
	WHERE rc.region = 'Europe'
	group by advisor_ent_id,rk.turnover) vt
	group by  vt.turnover
) vt3
on vt3.turnover = vt2.turnover"""

query2 = """
-- JOIN -- 
SELECT turnover, COUNT(distinct vt1.advisor_ent_id) AS client_1, country, COUNT(distinct vt3.advisor_ent_id) AS client_country
FROM (
-- CHECKED -- 
	SELECT 
		'AAR202207.1' AS project_code,
		rk.turnover, 
		or1.fund_ent_id,
		or1.advisor_ent_id
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].[ref].turnover_key rk ON rk.turnover_key = or1.turnover_key
	WHERE client_code = 'AAR' 
	AND project_code = 'AAR202207.1' 
	GROUP BY rk.turnover, fund_ent_id, advisor_ent_id) vt1
JOIN (
	SELECT 
	investor_country_key AS country_key_all_projects_1, 
	kc.country,
	advisor_ent_id,
	or1.fund_ent_id,
	'AAR202207.1' AS project_code,
	COUNT(distinct advisor_ent_id) AS client_1
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].ref.country_key kc
	on or1.investor_country_key = kc.country_key
	WHERE client_code = 'AAR' AND project_code = 'AAR202207.1'
	GROUP BY investor_country_key, kc.country, advisor_ent_id, or1.fund_ent_id
) vt3 
on vt1.advisor_ent_id = vt3.advisor_ent_id
group by turnover, country
"""

query3 = """
SELECT turnover, COUNT(distinct vt1.advisor_ent_id) AS client_1, country, COUNT(distinct vt3.advisor_ent_id) AS client_country
FROM (
-- CHECKED -- 
	SELECT 
		'AAR202002.2' AS project_code,
		rk.turnover, 
		or1.fund_ent_id,
		or1.advisor_ent_id
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].[ref].turnover_key rk ON rk.turnover_key = or1.turnover_key
	WHERE client_code = 'AAR' 
	AND project_code = 'AAR202002.2' 
	GROUP BY rk.turnover, fund_ent_id, advisor_ent_id) vt1
JOIN (
	SELECT 
	investor_country_key AS country_key_all_projects_1, 
	kc.country,
	advisor_ent_id,
	or1.fund_ent_id,
	'AAR202002.2' AS project_code,
	COUNT(distinct advisor_ent_id) AS client_1
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].ref.country_key kc
	on or1.investor_country_key = kc.country_key
	WHERE client_code = 'AAR' AND project_code = 'AAR202002.2'
	GROUP BY investor_country_key, kc.country, advisor_ent_id, or1.fund_ent_id
) vt3 
on vt1.advisor_ent_id = vt3.advisor_ent_id
group by turnover, country
"""

query4 = """
SELECT turnover, COUNT(distinct vt1.advisor_ent_id) AS client_1, country, COUNT(distinct vt3.advisor_ent_id) AS client_country
FROM (
-- CHECKED -- 
	select count(distinct vt.advisor_ent_id) as client_europe
		, turnover
		, advisor_ent_id
from (
	SELECT max(report_yr) as max_year
		, advisor_ent_id
		, rk.turnover
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].[ref].turnover_key rk ON rk.turnover_key = or1.turnover_key
	JOIN [CDB].ref.country_key kc on or1.investor_country_key = kc.country_key
	JOIN [CDB].ref.region_key rc on rc.region_key = kc.region_key
	WHERE rc.region = 'Europe'
	group by advisor_ent_id,rk.turnover) vt
	group by  vt.turnover, advisor_ent_id) vt1
JOIN (
	SELECT 
	country_key_all_projects_3, 
	country,
	advisor_ent_id,
	COUNT(distinct advisor_ent_id) AS client_1
	from (
	SELECT max(report_yr) as max_year
		, advisor_ent_id
		, kc.country
		, investor_country_key AS country_key_all_projects_3
	FROM [CDB].[own].[reported_0] or1
	JOIN [CDB].ref.country_key kc on or1.investor_country_key = kc.country_key
	JOIN [CDB].ref.region_key rc on rc.region_key = kc.region_key
	WHERE rc.region = 'Europe'
	group by advisor_ent_id, kc.country, investor_country_key ) vt
	group by country_key_all_projects_3, advisor_ent_id, country
) vt3 
on vt1.advisor_ent_id = vt3.advisor_ent_id
group by turnover, country
"""

turnover_df = pd.read_sql(query, cnxn)
turnover_country1_df = pd.read_sql(query2, cnxn)
turnover_country2_df = pd.read_sql(query3, cnxn)
turnover_country3_df = pd.read_sql(query4, cnxn)

print('2 var db' + str(turnover_country1_df.head()))
print('2 var db' + str(turnover_country2_df.head()))

# plt.bar(turnover_country_df['turnover'], turnover_country_df['client_1'])
# plt.xlabel('Turnover')
# plt.ylabel('Number of Clients')
# plt.show()

#################################################################################################################################################################

total_client_curr = turnover_df['client_curr'].sum()
turnover_df['client_curr_pct'] = round((turnover_df['client_curr'] / total_client_curr) * 100)
total_client_old = turnover_df['client_old'].sum()
turnover_df['client_old_pct'] = round((turnover_df['client_old'] / total_client_old) * 100)
total_client_europe = turnover_df['client_europe'].sum()
turnover_df['client_europe_pct'] = round((turnover_df['client_europe'] / total_client_europe) * 100)

# Pie charts
fig1 = go.Figure(go.Pie(
    labels=turnover_df['profile_var'],
    values=turnover_df['client_curr'],
    name='Client New Turnover'
))
fig1.update_layout(title='Client New Turnover Distribution')
fig1.update_traces(ids=['chart1'])

fig2 = go.Figure(go.Pie(
    labels=turnover_df['profile_var'],
    values=turnover_df['client_old'],
    name='Client Old Turnover'
))
fig2.update_layout(title='Client Old Turnover Distribution')
fig2.update_traces(ids=['chart2'])

turnover_df['pct_change_curr'] = turnover_df['client_europe_pct'] - turnover_df['client_curr_pct']

fig3 = go.Figure(go.Pie(
    labels=turnover_df['profile_var'],
    values=turnover_df['client_europe'],
    name='Client Europe Turnover'
))
fig3.update_layout(title='Client Europe Turnover Distribution')
fig3.update_traces(ids=['chart3'])

fig1.update_layout(title='Client New Turnover Distribution', width=600, height=600, margin=dict(t=50, b=0, l=0, r=0))
fig2.update_layout(title='Client Old Turnover Distribution', width=600, height=600, margin=dict(t=50, b=0, l=0, r=0))
fig3.update_layout(title='Client Europe Turnover Distribution', width=600, height=600, margin=dict(t=50, b=0, l=0, r=0))

#bar charts

fig7 = go.Figure()

fig7.add_trace(
    go.Bar(x=turnover_df['profile_var'], y=turnover_df['pct_change_curr'],
           marker_color=['#EF553B' if x < 0 else '#00CC96' for x in turnover_df['pct_change_curr']],
           text=[f'{val:1}' for val in turnover_df['pct_change_curr']],
           hovertemplate='Profile Variable: %{x}<br>Percentage Change: %{y}%<extra></extra>')
)

fig7.update_layout(
    title='Percentage Change in Turnover of Current Client vs Europe',
    xaxis_title='Profile Variable',
    yaxis_title='Percentage Change'
)
#############################################################################################################################################################
heatmap_data = [
    go.Heatmap(
        z=turnover_country1_df['client_1'],
        x=turnover_country1_df['turnover'],
        y=turnover_country1_df['country'],
        colorscale='Viridis',
        reversescale=True
    )
]

heatmap_layout = go.Layout(
    title='Client Turnover by Advisor and Country',
    xaxis=dict(title='Advisor Entity ID'),
    yaxis=dict(title='Country 1'),
    margin=dict(l=100, r=20, t=70, b=70),
    height=600,
    width = 600
)

heatmap_fig = go.Figure(data=heatmap_data, layout=heatmap_layout)

#############################################################################################################################################################
heatmap_data2 = [
    go.Heatmap(
        z=turnover_country2_df['client_1'],
        x=turnover_country2_df['turnover'],
        y=turnover_country2_df['country'],
        colorscale='Viridis',
        reversescale=True
    )
]

heatmap_layout = go.Layout(
    title='Client Turnover by Advisor and Country',
    xaxis=dict(title='Advisor Entity ID'),
    yaxis=dict(title='Country 2'),
    margin=dict(l=100, r=20, t=70, b=70),
    height=600,
    width = 600
)

heatmap_fig2 = go.Figure(data=heatmap_data2, layout=heatmap_layout)

#############################################################################################################################################################
heatmap_data3 = [
    go.Heatmap(
        z=turnover_country3_df['client_1'],
        x=turnover_country3_df['turnover'],
        y=turnover_country3_df['country'],
        colorscale='Viridis',
        reversescale=True
    )
]

heatmap_layout = go.Layout(
    title='Client Turnover by Advisor and Country',
    xaxis=dict(title='Advisor Entity ID'),
    yaxis=dict(title='Country 2'),
    margin=dict(l=100, r=20, t=70, b=70),
    height=600,
    width = 600
)

heatmap_fig3 = go.Figure(data=heatmap_data3, layout=heatmap_layout)
#############################################################################################################################################################
# Define the options for the dropdown
options = [{'label': var, 'value': var} for var in turnover_df['profile_var'].unique()]
values = [option['value'] for option in options]

print(options)

# Create dashboard
app = dash.Dash(__name__)

app.layout = html.Div([
    # Add the dropdown filter to the dashboard
    html.H1("Investor Turnover Dashboard",
            style={'font-size': '24px', 'font-family': 'sans-serif', 'text-align': 'center'}),
    html.H3('Filter by Profile Variable',
            style={'font-size': '16px', 'font-family': 'sans-serif', 'text-align': 'center'}),
    dcc.Dropdown(
        id='profile-dropdown',
        options=options,
        value=[opt['value'] for opt in options],
        multi=True,
        style={'text-align': 'left', 'width': '90%', 'margin': '10px'}),
    html.Div([
        dcc.Graph(id='bar-chart', figure=fig7)
    ]),
    html.Div([
        dcc.Graph(id="heatmap2", figure=heatmap_fig)
        # dcc.Dropdown(
        #     id='country',
        #     options=[{'label': c, 'value': c} for c in turnover_country_df['country'].unique()],
        #     value=turnover_country_df['country'].unique()[0]
        # )
    ]),
    html.Div([
            dcc.Graph(id="heatmap", figure=heatmap_fig2)
            # dcc.Dropdown(
            #     id='country',
            #     options=[{'label': c, 'value': c} for c in turnover_country_df['country'].unique()],
            #     value=turnover_country_df['country'].unique()[0]
            # )
        ]),
    html.Div([
            dcc.Graph(id="heatmap", figure=heatmap_fig3)
            # dcc.Dropdown(
            #     id='country',
            #     options=[{'label': c, 'value': c} for c in turnover_country_df['country'].unique()],
            #     value=turnover_country_df['country'].unique()[0]
            # )
        ]),
    html.Div(
        dcc.Graph(id='chart1', figure=fig1),
        className='four columns',
        style={'text-align': 'center'}
    ),
    html.Div(
        dcc.Graph(id='chart2', figure=fig2),
        className='four columns',
        style={'text-align': 'center'}
    ),
    html.Div(
        dcc.Graph(id='chart3', figure=fig3),
        className='four columns',
        style={'text-align': 'center'}
    ),
], className='row', style={'margin-bottom': '10px'})


def update_charts(selected_var):
    filtered_df = turnover_df[turnover_df['profile_var'].isin(selected_var)]

    total_client_curr_filtered = filtered_df['client_curr'].sum()
    turnover_df['client_curr_pct_filtered'] = round((filtered_df['client_curr'] / total_client_curr_filtered) * 100)
    #print(str(turnover_df['client_curr_pct_filtered']))
    total_client_old_filtered = filtered_df['client_old'].sum()
    turnover_df['client_old_pct_filtered'] = round((filtered_df['client_old'] / total_client_old_filtered) * 100)
    #print(str(turnover_df['client_old_pct_filtered']))
    total_client_europe_filtered = filtered_df['client_europe'].sum()
    turnover_df['client_europe_pct_filtered'] = round((filtered_df['client_europe'] / total_client_europe_filtered) * 100)
    #print(str(turnover_df['client_europe_pct_filtered']))

    turnover_df['pct_change_curr'] = turnover_df['client_europe_pct_filtered'] - turnover_df['client_curr_pct_filtered']
    #print('differences: ' + str(turnover_df['pct_change_curr'] ))

    labels = labels = [f"{pct:.2f}%" for pct in turnover_df['pct_change_curr']]
    colors = ['green' if pct >= 0 else 'red' for pct in turnover_df['pct_change_curr']]

    fig1 = go.Figure()
    fig1.add_trace(
        go.Pie(labels=filtered_df['profile_var'], values=filtered_df['client_curr_pct_filtered'], name='Client New Turnover'))
    fig1.update_layout(title='Client New Distribution')

    fig2 = go.Figure()
    fig2.add_trace(
        go.Pie(labels=filtered_df['profile_var'], values=filtered_df['client_europe_pct_filtered'], name='Client Old Turnover'))
    fig2.update_layout(title='Client Old Distribution')

    fig3 = go.Figure()
    fig3.add_trace(
        go.Pie(labels=filtered_df['profile_var'], values=filtered_df['client_europe'], name='Client Europe Turnover'))
    fig3.update_layout(title='Client Europe Distribution')

    fig7.update_traces(
        x=turnover_df['profile_var'],
        y=turnover_df['pct_change_curr'],
        text=labels,
        marker=dict(color=colors),
        textposition='auto'
    )
    fig7.update_layout(
        title='Percentage Change in Turnover of Current Client vs Europe',
        xaxis_title='Profile Variable',
        yaxis_title='Percentage Change'
    )
    return [fig1, fig2, fig3], fig7


@app.callback(
    [Output('chart1', 'figure'),
     Output('chart2', 'figure'),
     Output('chart3', 'figure'),
     Output('bar-chart', 'figure')],
    [Input('profile-dropdown', 'value')]
)
def update_dashboard(selected_var):
    figs, fig = update_charts(selected_var)
    return figs[0], figs[1], figs[2], fig7


if __name__ == '__main__':
    app.run_server(debug=True)
