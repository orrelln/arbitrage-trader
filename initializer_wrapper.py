from arbitrage.initializer import Initializer
from scripts.wrappers import indef_call


def init_loop(initializer_obj):
    initializer_obj.initialize_exchanges()
    initializer_obj.initialize_pairs()
    initializer_obj.write_to_database()


def main():
    initializer_obj = Initializer()
    indef_call(init_loop, 1200, initializer_obj)


if __name__ == '__main__':
    main()