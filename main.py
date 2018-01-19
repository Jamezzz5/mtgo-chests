import sys
import logging
import chests.io as io


formatter = logging.Formatter('%(asctime)s [%(module)14s]' +
                              '[%(levelname)8s] %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

console = logging.StreamHandler(sys.stdout)
console.setFormatter(formatter)
log.addHandler(console)

log_file = logging.FileHandler('logfile.log', mode='w')
log_file.setFormatter(formatter)
log.addHandler(log_file)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught exception: ",
                     exc_info=(exc_type, exc_value, exc_traceback))


def main():
    io.get_chest_info()
    df = io.get_curated_card_df()
    df = io.get_card_prices_curated_cards(df)
    cur_val = io.calculate_curated_ev(df)
    pp_val = io.calculate_pp_ev()
    io.calculate_chest_ev(cur_val, pp_val)


if __name__ == '__main__':
    main()
