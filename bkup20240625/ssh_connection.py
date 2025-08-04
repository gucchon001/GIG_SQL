from sshtunnel import SSHTunnelForwarder
import traceback

def create_ssh_tunnel(ssh_config):
    try:
        tunnel = SSHTunnelForwarder(
            (ssh_config['host'], 22),
            ssh_username=ssh_config['user'],
            ssh_private_key=ssh_config['ssh_key_path'],
            remote_bind_address=(ssh_config['db_host'], ssh_config['db_port']),
            local_bind_address=('127.0.0.1', ssh_config['local_port'])
        )
        tunnel.start()
        print('SSHトンネルを開設しました。')
        return tunnel
    except Exception as e:
        print(f"SSHトンネルエラー: {e}")
        traceback.print_exc()
        return None
