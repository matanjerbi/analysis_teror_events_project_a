from flask import Blueprint, jsonify, request
import pandas as pd
import folium
import branca.colormap as cm
from services.load_data_to_data_frame import load_data


stat_queries = Blueprint('stat_attack', __name__)

#  1. סוגי ההתקפה הקטלניים ביותר
@stat_queries.route('/deadliest_attacks', methods=['GET'])
def deadliest_attacks():
    global df
    df = load_data()
    attack_stats = df.groupby('attack_type').agg({
        'score': 'sum',
        'killed': 'sum',
        'wounded': 'sum'
    }).sort_values('score', ascending=False)

    result = {
        attack_type: {
            'total_score': int(row['score']),
            'total_killed': int(row['killed']),
            'total_wounded': int(row['wounded'])
        }
        for attack_type, row in attack_stats.head().iterrows()
    }

    sorted_result = dict(sorted(result.items(), key=lambda x: x[1]['total_score'], reverse=True))

    return jsonify(sorted_result)

# 2. ממוצע נפגעים לפי אזור
@stat_queries.route('/casualties_by_region')
def casualties_by_region():
    # Calculate average coordinates for each region
    region_stats = df.groupby('region').agg({
        'score': 'mean',
        'latitude': 'mean',  # Changed from 'first' to 'mean'
        'longitude': 'mean'  # Changed from 'first' to 'mean'
    }).reset_index()

    m = folium.Map(location=[0, 0], zoom_start=2)

    colormap = cm.LinearColormap(
        colors=['yellow', 'orange', 'red'],
        vmin=region_stats['score'].min(),
        vmax=region_stats['score'].max(),

    )

    for _, row in region_stats.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=min(row['score'], 50),  # Cap maximum size
                popup=f"{row['region']}: {row['score']:.2f}",
                color=colormap(row['score']),
                fill=True,
                fill_opacity=2
            ).add_to(m)

    colormap.add_to(m)
    return m._repr_html_()


# 3. חמש הקבוצות עם הכי הרבה נפגעים
@stat_queries.route('/top_deadly_groups')
def top_deadly_groups():
    group_stats = df.groupby('group').agg({
        'score': 'sum',
        'killed': 'sum',
        'wounded': 'sum',
    }).sort_values('score', ascending=False)

    top_5_groups = group_stats.head()

    result = {
        group: {
            'total_score': int(row['score']),
            'total_killed': int(row['killed']),
            'total_wounded': int(row['wounded'])
        }
        for i, (group, row) in enumerate(top_5_groups.iterrows())
    }

    sorted_result = dict(sorted(result.items(), key=lambda x: x[1]['total_score'], reverse=True))

    return jsonify(sorted_result)


# 6. הבדל בין שני שנים באחוז הנפגעים לפי שנה ואזור
@stat_queries.route('/change_between_years/<int:start_year>/<int:end_year>')
def change_between_years(start_year, end_year):
    # Get yearly counts per region
    yearly_counts = df.groupby(['region', df['date'].dt.year]).size().unstack(fill_value=0)
    yearly_changes = yearly_counts.pct_change(axis=1) * 100

    # Get coordinates
    region_coords = df.groupby('region').agg({
        'latitude': 'mean',
        'longitude': 'mean'
    })

    m = folium.Map(location=[0, 0], zoom_start=2)

    for region in yearly_changes.index:
        if region in region_coords.index:
            coords = region_coords.loc[region]
            if pd.notna(coords['latitude']) and pd.notna(coords['longitude']):
                avg_change = yearly_changes.loc[region].mean()

                changes_html = '<table style="width:100%"><tr><th>Period</th><th>Change</th></tr>'

                for year in range(start_year, end_year):
                    if year in yearly_changes.columns:
                        change = yearly_changes.loc[region, year]
                        changes_html += f"""
                            <tr>
                                <td>{year - 1} → {year}</td>
                                <td>{change:.1f}%</td>
                            </tr>
                        """

                popup_html = f"""
                    <div style="max-height: 300px; overflow-y: auto;">
                        <h3>{region}</h3>
                        <h4>Yearly Changes</h4>
                        {changes_html}
                        </table>
                    </div>
                """

                folium.CircleMarker(
                    location=[coords['latitude'], coords['longitude']],
                    radius=min(abs(avg_change) / 2, 30),
                    popup=folium.Popup(popup_html, max_width=300),
                    color='red' if avg_change > 0 else 'blue',
                    fill=True,
                    fill_opacity=0.7
                ).add_to(m)

    return m._repr_html_()


# 8. הקבוצות הפעילות ביותר באזור
@stat_queries.route('/active_groups_by_region/<region>')
def active_groups_by_region(region):
    top_n = request.args.get('top', default=5, type=int)

    region_data = df[df['region'] == region]

    if len(region_data) == 0:
        return jsonify({'error': f'No data found for region: {region}'})

    top_groups = region_data['group'].value_counts().head(top_n)

    m = folium.Map(location=[0, 0], zoom_start=2)

    avg_lat = region_data['latitude'].mean()
    avg_lng = region_data['longitude'].mean()

    colors = ['red', 'blue', 'green', 'purple', 'orange', 'yellow', 'cyan', 'magenta']

    for i, (group, count) in enumerate(top_groups.items()):
        group_data = region_data[region_data['group'] == group]

        for _, attack in group_data.iterrows():
            if pd.notna(attack['latitude']) and pd.notna(attack['longitude']):
                folium.CircleMarker(
                    location=[attack['latitude'], attack['longitude']],
                    radius=8,
                    popup=f"{group}<br>Attack: {attack['attack_type']}<br>Score: {attack['score']}",
                    color=colors[i % len(colors)],
                    fill=True
                ).add_to(m)

    legend_html = '<div style="position: fixed; bottom: 50px; right: 50px; background-color: white; padding: 10px; border: 1px solid grey; z-index: 1000;">'
    for i, group in enumerate(top_groups.index):
        legend_html += f'<p><span style="color:{colors[i % len(colors)]}">●</span> {group}: {top_groups[group]} attacks</p>'
    legend_html += '</div>'

    m.get_root().html.add_child(folium.Element(legend_html))

    if pd.notna(avg_lat) and pd.notna(avg_lng):
        m.location = [avg_lat, avg_lng]
        m.zoom_start = 6

    return m._repr_html_()

