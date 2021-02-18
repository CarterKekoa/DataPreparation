import os
import copy

from mypytable import MyPyTable 

def main():
    dataset_prep_duplicates("auto-mpg.txt", "auto-mpg-nodups.txt")
    dataset_prep_duplicates("auto-prices.txt", "auto-prices-nodups.txt")
    outer_table = dataset_prep_join("auto-mpg-clean.txt", "auto-prices-clean.txt", "auto-data.txt")
    dataset_prep_stats(outer_table)
    dataset_prep_missing_values(outer_table, "auto-data-removed-NA.txt")
    dataset_prep_missing_values2(outer_table, "auto-data-replaced-NA.txt")

    pass    

def dataset_prep_duplicates(filename, save_filename):
    mpg_fname = os.path.join("input_data", filename)
    table = MyPyTable().load_from_file(mpg_fname)
    print("//////////////////////////////////////////////////")
    print(filename, ":")
    print("--------------------------------------------------")
    N, M = MyPyTable.get_shape(table)
    print("No. of instances:", N, "x", M)
    key_col_names = ["car name", "model year"]
    duplicates = MyPyTable.find_duplicates(table, key_col_names)
    print("Duplicates:", duplicates)
    print()
    print("--------------------------------------------------")
    MyPyTable.drop_rows(table, duplicates)
    MyPyTable.save_to_file(table, save_filename)
    duplicates = MyPyTable.find_duplicates(table, key_col_names)
    print("duplicates removed (saved as", save_filename, "):")
    print("--------------------------------------------------")
    N, M = MyPyTable.get_shape(table)
    print("No. of instances:", N, "x", M)
    print("Duplicates:", duplicates)
    print()
    print()
    pass

def dataset_prep_join(filename, filename2, save_filename):
    """Comment Block on how and why I resolved the missing mpg cases in auto-...-nodups
    
    Manual Edits:
        * 'audi 100 ls' year changed from 70 -> 71 * caused a NA in all cols
        * 'bmw 2002' year changed from 70 -> 71 * caused a NA in all cols
        * 'chevy c20' year changed from 70 -> 71 * caused a NA in all cols
        * 'dodge d200' year changed from 70 -> 71 * caused a NA in all cols
        * 'ford f250' year changed from 70 -> 71 * caused a NA in all cols
        * 'peugeot 504' year changed from 70 -> 71 * caused a NA in all cols
        * 'saab 99e' year changed from 70 -> 71 * caused a NA in all cols
        * 'toyota corona mark ii' name changed from 'toyoto' * caused a NA in all cols
    
    """
    print("//////////////////////////////////////////////////")
    key_col_names = ["car name", "model year"]

    mpg_fname = os.path.join("input_data", filename)
    table = MyPyTable().load_from_file(mpg_fname)
    print(filename, ":")
    print("--------------------------------------------------")
    N, M = MyPyTable.get_shape(table)
    duplicates = MyPyTable.find_duplicates(table, key_col_names)
    print("No. of instances:", N, "x", M)
    print("Duplicates:", duplicates)
    print("--------------------------------------------------")

    mpg_fname2 = os.path.join("input_data", filename2)
    table2 = MyPyTable().load_from_file(mpg_fname2)
    print(filename2, ":")
    print("--------------------------------------------------")
    N2, M2 = MyPyTable.get_shape(table2)
    duplicates2 = MyPyTable.find_duplicates(table2, key_col_names)
    print("No. of instances:", N2, "x", M2)
    print("Duplicates:", duplicates2)
    print()
    print()

    print("//////////////////////////////////////////////////")
    outer_table = MyPyTable.perform_full_outer_join(table, table2, key_col_names)
    MyPyTable.save_to_file(outer_table, save_filename)
    print("combined table (saved as", save_filename, "):")
    print("--------------------------------------------------")
    N, M = MyPyTable.get_shape(outer_table)
    duplicates = MyPyTable.find_duplicates(outer_table, key_col_names)
    print("No. of instances:", N, "x", M)
    print("Duplicates:", duplicates)
    print()
    print()
    return outer_table

def dataset_prep_stats(table):
    print("//////////////////////////////////////////////////")
    print("Summary Stats:")
    stats_copy = copy.deepcopy(table)
    col_names = ["mpg", "displacement", "horsepower", "weight", "acceleration",
                "model year", "msrp"]
    stats_table = MyPyTable.compute_summary_statistics(stats_copy, col_names)
    MyPyTable.pretty_print(stats_table)
    print()
    print()
    pass

def dataset_prep_missing_values(table, save_filename):
    remove_copy = copy.deepcopy(table)
    key_col_names = ["car name", "model year"]
    print("//////////////////////////////////////////////////")
    MyPyTable.remove_rows_with_missing_values(remove_copy)
    MyPyTable.save_to_file(remove_copy, save_filename)
    print("combined table - rows w/missing values removed (saved as", save_filename,")")
    print("--------------------------------------------------")
    N, M = MyPyTable.get_shape(remove_copy)
    duplicates = MyPyTable.find_duplicates(remove_copy, key_col_names)
    print("No. of instances:", N, "x", M)
    print("Duplicates:", duplicates)
    dataset_prep_stats(remove_copy)
    pass

def dataset_prep_missing_values2(table2, save_filename):
    replace_copy = copy.deepcopy(table2)
    key_col_names2 = ["car name", "model year"]
    print("//////////////////////////////////////////////////")
    MyPyTable.replace_missing_values_with_column_average(replace_copy, "mpg")
    MyPyTable.replace_missing_values_with_column_average(replace_copy, "horsepower")
    MyPyTable.replace_missing_values_with_column_average(replace_copy, "msrp")
    MyPyTable.save_to_file(replace_copy, save_filename)
    print("combined table - rows w/missing values replaced (saved as", save_filename,")")
    print("--------------------------------------------------")
    N2, M2 = MyPyTable.get_shape(replace_copy)
    duplicates2 = MyPyTable.find_duplicates(replace_copy, key_col_names2)
    print("No. of instances:", N2, "x", M2)
    print("Duplicates:", duplicates2)
    dataset_prep_stats(replace_copy)
    pass

if __name__ == "__main__":
    main()