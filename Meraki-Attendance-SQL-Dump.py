import os
from datetime import datetime, time, timezone, timedelta
import pyodbc
import meraki

API_KEY = os.environ.get('MERAKI_API_KEY')
if not API_KEY:
    raise ValueError("Environment variable 'MERAKI_API_KEY' is not set!")

def calculate_timespan_seconds(current_time):
    midnight = datetime.combine(current_time.date(), time(), tzinfo=timezone.utc)
    delta = current_time - midnight
    return int(delta.total_seconds())

def get_description(client):
    description = client.get('description')
    return description if description is not None else ''

def get_worksheet_name(network):
    name = network["name"]
    return name[:31]

def filter_usage_history(usage_history, days_back):
    """
    Filters the usage history to return entries within the specified number of days.
    """
    filtered_history = []
    threshold_date = datetime.now() - timedelta(days=days_back)

    for entry in usage_history:
        entry_date = datetime.fromisoformat(entry['ts'].rstrip('Z'))
        if entry_date >= threshold_date:
            filtered_history.append(entry)
    
    return filtered_history

def get_all_client_usages(dashboard, network_id, client_id):
    usage_history = dashboard.networks.getNetworkClientUsageHistory(network_id, client_id)
    return usage_history

def update_or_insert_into_database(cursor, network_name, date, client_description, login_name, total_usage, meraki_id, mac, ip):
    check_query = """
        SELECT COUNT(*) FROM Attendance
        WHERE NetworkName = ? AND Date = ? AND MerakiID = ?
    """
    cursor.execute(check_query, (network_name, date, meraki_id))
    count = cursor.fetchone()[0]
    
    if count == 0:
        insert_query = """
            INSERT INTO Attendance (NetworkName, Date, Description, [User], Usage, MerakiID, Mac, IP)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (network_name, date, client_description, login_name, total_usage, meraki_id, mac, ip))
    else:
        update_query = """
            UPDATE Attendance SET Description = ?, [User] = ?, Usage = ?, Mac = ?, IP = ?
            WHERE NetworkName = ? AND Date = ? AND MerakiID = ?
        """
        cursor.execute(update_query, (client_description, login_name, total_usage, mac, ip, network_name, date, meraki_id))

def main():
    dashboard = meraki.DashboardAPI(api_key=API_KEY, base_url='https://api.meraki.com/api/v1/', output_log=True,
                                    log_file_prefix=os.path.basename(__file__)[:-3], log_path='D:\\ScheduledTasks\\Meraki_Attendance_Report\\Logs-SQL', print_console=False)

    organizations = dashboard.organizations.getOrganizations()
    target_networks = [network for org in organizations for network in dashboard.organizations.getOrganizationNetworks(org['id'])
                       if any(device['model'].startswith('MR') or device['model'].startswith('MS') for device in dashboard.networks.getNetworkDevices(network['id']))]

    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=ServerName-DB01\DB01;'  
        r'DATABASE=MerakiReports;'
        r'UID=svc_merakireports;'
        r'PWD=PASSWORD;'
        r'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    for net in target_networks:
        network_name = get_worksheet_name(net)
        clients = dashboard.networks.getNetworkClients(net['id'], total_pages='all')

        for backfill_days in range(2):  # Number of previous days to backfill (Today Inclusive)
            current_day = datetime.now().replace(tzinfo=timezone.utc) - timedelta(days=backfill_days)
            current_day_str = current_day.strftime('%Y-%m-%d')

            for client in clients:
                client_description = get_description(client)
                login_name = client.get('user', 'N/A')
                meraki_id = client.get('id', "N/A")
                mac = client.get('mac', "N/A")
                ip = client.get('ip', "N/A")

                all_usages = get_all_client_usages(dashboard, net['id'], client['id'])
                filtered_usages = filter_usage_history(all_usages, backfill_days + 1)

                for usage in filtered_usages:
                    received = usage.get('received', 0) or 0
                    sent = usage.get('sent', 0) or 0
                    total_usage = received + sent
                    update_or_insert_into_database(cursor, network_name, current_day_str, client_description, login_name, total_usage, meraki_id, mac, ip)

    conn.commit()
    cursor.close()
    conn.close()

    print('\nScript complete')

if __name__ == '__main__':
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    print(f'Total runtime: {end_time - start_time}')
