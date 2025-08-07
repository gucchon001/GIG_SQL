"""
SSH トンネル管理

セキュアなデータベース接続のためのSSHトンネル機能を提供
"""
from sshtunnel import SSHTunnelForwarder
import traceback
from typing import Optional, Dict, Any
from src.core.logging.logger import get_logger


class SSHTunnel:
    """SSH トンネル管理クラス"""
    
    def __init__(self, ssh_config: Dict[str, Any]):
        """
        SSH トンネル管理を初期化
        
        Args:
            ssh_config: SSH設定辞書
        """
        self.ssh_config = ssh_config
        self.logger = get_logger(__name__)
        self.tunnel: Optional[SSHTunnelForwarder] = None
        
    def start(self) -> bool:
        """
        SSH トンネルを開始
        
        Returns:
            bool: 成功時True、失敗時False
        """
        try:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_config['host'], 22),
                ssh_username=self.ssh_config['user'],
                ssh_private_key=self.ssh_config['ssh_key_path'],
                remote_bind_address=(
                    self.ssh_config.get('db_host', '127.0.0.1'),
                    self.ssh_config.get('db_port', 3306)
                ),
                local_bind_address=(
                    '127.0.0.1',
                    self.ssh_config.get('local_port', 3306)
                )
            )
            
            self.tunnel.start()
            self.logger.info(f'SSHトンネルを開設しました: {self.tunnel.local_bind_host}:{self.tunnel.local_bind_port}')
            return True
            
        except Exception as e:
            self.logger.error(f"SSHトンネルエラー: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def stop(self) -> None:
        """SSH トンネルを停止"""
        if self.tunnel:
            try:
                self.tunnel.stop()
                self.logger.info('SSHトンネルを停止しました')
            except Exception as e:
                self.logger.error(f"SSHトンネル停止エラー: {e}")
            finally:
                self.tunnel = None
    
    def is_active(self) -> bool:
        """
        SSH トンネルがアクティブかチェック
        
        Returns:
            bool: アクティブならTrue
        """
        return self.tunnel is not None and self.tunnel.is_active
    
    def get_local_bind_port(self) -> Optional[int]:
        """
        ローカルバインドポートを取得
        
        Returns:
            int: ローカルバインドポート（トンネルが非アクティブの場合はNone）
        """
        if self.tunnel and self.tunnel.is_active:
            return self.tunnel.local_bind_port
        return None
    
    def restart(self) -> bool:
        """
        SSH トンネルを再起動
        
        Returns:
            bool: 成功時True、失敗時False
        """
        self.logger.info("SSHトンネルを再起動します")
        self.stop()
        return self.start()
    
    def __enter__(self):
        """コンテキストマネージャー対応"""
        if self.start():
            return self
        raise Exception("SSHトンネルの開始に失敗しました")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャー対応"""
        self.stop()


# 後方互換性のための関数
def create_ssh_tunnel(ssh_config: Dict[str, Any]) -> Optional[SSHTunnelForwarder]:
    """
    旧式のSSHトンネル作成関数（後方互換性のため）
    
    Args:
        ssh_config: SSH設定辞書
        
    Returns:
        SSHTunnelForwarder: SSHトンネルオブジェクト（失敗時はNone）
    """
    tunnel_manager = SSHTunnel(ssh_config)
    if tunnel_manager.start():
        return tunnel_manager.tunnel
    return None