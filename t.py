from time import ctime, sleep
import schedule


def main():
    print(f"Main-{ctime()}")


schedule.every().second.do(main)

print(f"inicio {ctime()}")

while 1:
    schedule.run_pending()
    sleep(1)
