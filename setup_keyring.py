import keyring

if __name__ == "__main__":
    keyring.set_password("clickhouse_lab1", "username", "default")
    keyring.set_password("clickhouse_lab1", "password", "default")
