"""Update the Elemental Fertiziler Columns in the backend datasheet..."""
import isudatateam.cscap_utils as util

KGHA_LBA = 1.12085
BACKEND = "1CdPi6IEnO3T35t-OFQajKYzFLpu7hwOaWdL2FG1_kVA"


def float2(val):
    """type conversion"""
    if val is None:
        return 0
    return float(val)


def main():
    """Do Main Things"""
    sprclient = util.get_spreadsheet_client(util.get_config())

    spreadsheet = util.Spreadsheet(sprclient, BACKEND)
    spreadsheet.get_worksheets()
    sheet = spreadsheet.worksheets["Field Operations"]

    for entry in sheet.get_list_feed().entry:
        row = entry.to_dict()
        if row["operation"] != "fertilizer_synthetic":
            continue
        if (
            row["productrate"] is None
            or row["productrate"] == "-1.0"
            or row["productrate"] == ""
        ):
            option = "B"
            # Option B, values are in lbs per acre
            phosphoruse = 0.0
            potassium = 0.0
            nitrogen = float2(row["nitrogen"]) * KGHA_LBA
            if row["phosphorus"] is not None and float2(row["phosphorus"]) > 0:
                phosphoruse = float2(row["phosphorus"]) * KGHA_LBA
            if row["phosphate"] is not None and float2(row["phosphate"]) > 0:
                phosphoruse = float2(row["phosphate"]) * KGHA_LBA * 0.437
            if row["potassium"] is not None and float2(row["potassium"]) > 0:
                potassium = float2(row["potassium"]) * KGHA_LBA
            if row["potash"] is not None and float2(row["potash"]) > 0:
                potassium = float(row["potash"]) * KGHA_LBA * 0.830
            sulfur = float2(row["sulfur"]) * KGHA_LBA
            zinc = float2(row["zinc"]) * KGHA_LBA
            magnesium = float2(row["magnesium"]) * KGHA_LBA
            calcium = float2(row["calcium"]) * KGHA_LBA
            iron = float2(row["iron"]) * KGHA_LBA
        else:
            option = "A"
            # Option A, straight percentages
            prate = float2(row["productrate"])
            nitrogen = prate * float2(row["nitrogen"]) / 100.0
            phosphoruse = 0.0
            potassium = 0.0
            if row["phosphorus"] is not None and float2(row["phosphorus"]) > 0:
                phosphoruse = prate * float2(row["phosphorus"]) / 100.0
            if row["phosphate"] is not None and float2(row["phosphate"]) > 0:
                phosphoruse = prate * float2(row["phosphate"]) / 100.0 * 0.437
            if row["potassium"] is not None and float2(row["potassium"]) > 0:
                potassium = prate * float2(row["potassium"]) / 100.0
            if row["potash"] is not None and float2(row["potash"]) > 0:
                potassium = prate * float(row["potash"]) / 100.0 * 0.830
            sulfur = prate * float2(row["sulfur"]) / 100.0
            zinc = prate * float2(row["zinc"]) / 100.0
            magnesium = prate * float2(row["magnesium"]) / 100.0
            calcium = prate * float2(row["calcium"]) / 100.0
            iron = prate * float2(row["iron"]) / 100.0

        print(
            ("Option: %s\n    nitrogen: Old: %s -> New: %s")
            % (option, row["nitrogenelem"], nitrogen)
        )
        entry.set_value("nitrogenelem", "%.2f" % (nitrogen,))
        entry.set_value("phosphoruselem", "%.2f" % (phosphoruse,))
        entry.set_value("potassiumelem", "%.2f" % (potassium,))
        entry.set_value("sulfurelem", "%.2f" % (sulfur,))
        entry.set_value("zincelem", "%.2f" % (zinc,))
        entry.set_value("magnesiumelem", "%.2f" % (magnesium,))
        entry.set_value("calciumelem", "%.2f" % (calcium,))
        entry.set_value("ironelem", "%.2f" % (iron,))
        # if option == 'B':
        util.exponential_backoff(sprclient.update, entry)


if __name__ == "__main__":
    main()
