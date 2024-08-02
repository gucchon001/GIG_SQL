import streamlit as st
import yaml
from modules.dashboard import Dashboard

def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

def main():
    config = load_config("bi_config.yaml")
    dashboard = Dashboard(config)
    dashboard.run()

if __name__ == "__main__":
    main()