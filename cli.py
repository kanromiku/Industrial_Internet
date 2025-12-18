import subprocess
import click
import configparser
import psycopg2
from psycopg2 import sql

@click.group()
def cli():
    """IndusNet 项目的命令行管理工具。"""
    pass

@cli.command()
def start_server():
    """启动 TCP 数据接收服务器。"""
    click.echo("正在启动服务器...")
    try:
        # 使用 subprocess 运行 server.py
        # 这使得服务器在前台运行，可以看到所有日志输出
        subprocess.run(["python", "server.py"], check=True)
    except FileNotFoundError:
        click.secho("错误: 'python' 命令未找到。请确保 Python 已安装并配置在系统路径中。", fg="red")
    except subprocess.CalledProcessError as e:
        click.secho(f"服务器运行出错: {e}", fg="red")
    except KeyboardInterrupt:
        click.echo("\n服务器已手动停止。")


@cli.command()
def start_simulator():
    """启动客户端设备模拟器。"""
    click.echo("正在启动客户端模拟器...")
    try:
        subprocess.run(["python", "client_simulator.py"], check=True)
    except FileNotFoundError:
        click.secho("错误: 'python' 命令未找到。请确保 Python 已安装并配置在系统路径中。", fg="red")
    except subprocess.CalledProcessError as e:
        click.secho(f"客户端模拟器运行出错: {e}", fg="red")
    except KeyboardInterrupt:
        click.echo("\n客户端模拟器已手动停止。")


@cli.command()
@click.option('--config-file', default='config.ini', help='配置文件路径。')
@click.option('--sql-file', default='db_init.sql', help='SQL 初始化脚本路径。')
def init_db(config_file, sql_file):
    """根据 SQL 文件初始化数据库。"""
    click.echo("正在初始化数据库...")

    config = configparser.ConfigParser()
    if not config.read(config_file):
        click.secho(f"错误: 无法读取配置文件 '{config_file}'。", fg="red")
        return

    try:
        dsn = config.get('database', 'dsn')
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        conn = psycopg2.connect(dsn)
        conn.autocommit = True  # 设置自动提交，以便执行DDL语句
        cur = conn.cursor()

        click.echo(f"成功连接到数据库。正在执行 '{sql_file}'...")
        cur.execute(sql_script)

        cur.close()
        conn.close()
        click.secho("数据库初始化成功！", fg="green")

    except configparser.NoSectionError:
        click.secho(f"错误: 配置文件 '{config_file}' 中缺少 [database] 部分。", fg="red")
    except configparser.NoOptionError:
        click.secho(f"错误: 配置文件 '{config_file}' 的 [database] 部分缺少 'dsn'。", fg="red")
    except FileNotFoundError:
        click.secho(f"错误: SQL 脚本 '{sql_file}' 未找到。", fg="red")
    except psycopg2.Error as e:
        click.secho(f"数据库操作失败: {e}", fg="red")
    except Exception as e:
        click.secho(f"发生未知错误: {e}", fg="red")


if __name__ == '__main__':
    cli()

