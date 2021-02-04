from mypytable import MyPyTable

import os
import pandas as pd 
import pytest

# test input data
header = ["id", "a", "b", "c"]
data = [["ID001", "1", "-5.5", 1.7],
            ["ID002", "A", "2.2", 1.0]]
data_as_numeric = [["ID001", 1, -5.5, 1.7],
            ["ID002", "A", 2.2, 1.0]]

header_dups = ["id", "a", "b"]
data_dups = [["ID001", "A", 1.0],
            ["ID002", "B", 1.5], 
            ["ID003", "A", 1.0],
            ["ID002", "A", 1.5],
            ["ID004", "C", 1.0],
            ["ID005", "C", 1.0],
            ["ID006", "D", 1.0],
            ["ID007", "A", 2.0],
            ["ID008", "C", 1.0]]

data_dups_dropped = [["ID001", "A", 1.0],
            ["ID002", "B", 1.5], 
            ["ID002", "A", 1.5],
            ["ID004", "C", 1.0],
            ["ID006", "D", 1.0],
            ["ID007", "A", 2.0]]

header_stats = ["a", "b", "c"]
data_stats = [[1.0, 2.0, 3.0],
                [2.5, 2.0, 1.0], 
                [0.0, -1.0, 1.0],
                [-2.0, 0.5, 0.0]]

# adapted from SQL examples at https://www.diffen.com/difference/Inner_Join_vs_Outer_Join
header_left = ["Product", "Price"]
data_left = [["Potatoes", 3],
                ["Avacodos", 4],
                ["Kiwis", 2],
                ["Onions", 1],
                ["Melons", 5],
                ["Oranges", 5],
                ["Tomatoes", 6]]
header_right = ["Product", "Quantity"]
data_right = [["Potatoes", 45],
                ["Avacodos", 63],
                ["Kiwis", 19],
                ["Onions", 20],
                ["Melons", 66],
                ["Broccoli", 27],
                ["Squash", 92]]

header_car_left = ["SaleId","EmployeeId","CarName","ModelYear","Amt"]
data_car_left = [[555,12,"ford pinto",75,3076],
                [556,12,"toyota corolla",75,2611],
                [998,13,"toyota corolla",75,2800],
                [999,12,"toyota corolla",76,2989]]
header_car_right = ["CarName","ModelYear","MSRP"]
data_car_right = [["ford pinto",75,2769],
                    ["toyota corolla",75,2711],
                    ["ford pinto",76,3025],
                    ["toyota corolla",77,2789]]

header_NAs = ["id", "a", "b"]
data_NAs = [["ID001", "A", 3.5],
            ["ID002", "B", "NA"], 
            ["ID003", "C", 1.0],
            ["ID004", "D", 1.5]]

# note: order is actual/received student value, expected/solution
# def test_table_init():
#     table = MyPyTable(header, data)

#     assert len(table.column_names) == len(header)
#     assert len(table.data) == 2

#     # make sure deep copies are made
#     # id() returns unique integer for objects in memory
#     # https://docs.python.org/3/library/functions.html#id
#     assert id(table.data) != id(data)
#     for i in range(len(table.data)):
#         assert id(table.data[i]) != id(data[i])

#     # test empty
#     table = MyPyTable([], [])
#     assert len(table.column_names) == 0
#     assert len(table.data) == 0

def test_get_shape():
    table = MyPyTable(header, data_as_numeric)

    shape = table.get_shape()
    assert shape[0] == len(data)
    assert shape[1] == len(header)

    # test empty
    table = MyPyTable([], [])
    assert len(table.column_names) == 0
    assert len(table.data) == 0

def test_get_column():
    table = MyPyTable(header, data_as_numeric)

    for i, col_label in enumerate(header):
        col = table.get_column(col_label)
        assert len(col) == len(data_as_numeric)
        
        for j in range(len(col)):
            assert col[j] == data_as_numeric[j][i]

    # test empty
    table = MyPyTable(header, [])
    col = table.get_column("a")
    assert len(col) == 0

def test_convert_to_numeric():
    table = MyPyTable(header, data)
    table.convert_to_numeric()

    assert len(table.data) == len(data_as_numeric)
    assert table.data == data_as_numeric

def test_drop_rows():
    table = MyPyTable(header_dups, data_dups)

    rows_to_drop = [["ID003", "A", 1.0],
                    ["ID005", "C", 1.0],
                    ["ID008", "C", 1.0]]
    table.drop_rows(rows_to_drop)
    assert table.data == data_dups_dropped

def test_load_from_file():
    fname = os.path.join("test", "dummy.csv")
    table = MyPyTable().load_from_file(fname)

    assert len(table.column_names) == len(header)
    assert len(table.data) == len(data_as_numeric)
    assert table.data == data_as_numeric

def test_save_to_file():
    fname = os.path.join("test", "dummy_out.csv")
    table = MyPyTable(header, data_as_numeric)

    table.save_to_file(fname)
    infile = open(fname, "r")
    lines = infile.readlines()

    assert len(lines) == len(data_as_numeric) + 1 # + 1 for header
    assert lines == ['id,a,b,c\n', 'ID001,1,-5.5,1.7\n', 'ID002,A,2.2,1.0\n']

def test_find_duplicates():
    table = MyPyTable(header_dups, data_dups)

    rows_to_drop = table.find_duplicates(["id"])
    assert rows_to_drop == [["ID002", "A", 1.5]]

    rows_to_drop = table.find_duplicates(["a", "b"])
    assert rows_to_drop == [["ID003", "A", 1.0],
                            ["ID005", "C", 1.0],
                            ["ID008", "C", 1.0]]

    # test empty
    table = MyPyTable(header_dups, [])
    rows_to_drop = table.find_duplicates(["a"])
    assert rows_to_drop == []

def test_remove_rows_with_missing_values():
    table = MyPyTable(header_NAs, data_NAs)

    table.remove_rows_with_missing_values()
    assert len(table.data) == 3

    removed_NAs = [["ID001", "A", 3.5],
            ["ID003", "C", 1.0],
            ["ID004", "D", 1.5]]
    assert table.data == removed_NAs

    # test on table w/o missing vals
    table = MyPyTable(header, data)

    table.remove_rows_with_missing_values()
    assert len(table.data) == 2
    assert table.data == data

def test_replace_missing_values_with_column_average():
    table = MyPyTable(header_NAs, data_NAs)
    
    table.replace_missing_values_with_column_average("b")
    assert len(table.data) == 4

    replaced_NAs = [["ID001", "A", 3.5],
            ["ID002", "B", 2.0], 
            ["ID003", "C", 1.0],
            ["ID004", "D", 1.5]]
    assert table.data == replaced_NAs

def test_compute_summary_statistics():
    table = MyPyTable(header_stats, data_stats)

    # even number of instances
    stats_table = table.compute_summary_statistics(["a", "c"])
    assert stats_table.data == [['a', -2.0, 2.5, 0.25, 0.375, 0.5], ['c', 0.0, 3.0, 1.5, 1.25, 1.0]]

    # odd number of instances
    data_stats_copy = data_stats.copy()
    data_stats_copy.pop(0)
    table = MyPyTable(header_stats, data_stats_copy)

    stats_table = table.compute_summary_statistics(["b"])
    assert stats_table.data == [['b', -1.0, 2.0, 0.5, 0.5, 0.5]]

    # test empty
    table = MyPyTable(header, [])
    stats_table = table.compute_summary_statistics(["b"])
    assert stats_table.data == []

def test_perform_inner_join():
    # single attribute key
    table_left = MyPyTable(header_left, data_left)
    table_right = MyPyTable(header_right, data_right)
    joined_table = table_left.perform_inner_join(table_right, ["Product"])

    assert len(joined_table.column_names) == 3
    # test against pandas' inner join
    df_left = pd.DataFrame(data_left, columns=header_left)
    df_right = pd.DataFrame(data_right, columns=header_right)
    df_joined = df_left.merge(df_right, how="inner", on=["Product"])
    assert joined_table.data == df_joined.values.tolist()

    # multiple attirbute key example from class
    table_left = MyPyTable(header_car_left, data_car_left)
    table_right = MyPyTable(header_car_right, data_car_right)
    joined_table = table_left.perform_inner_join(table_right, ["CarName", "ModelYear"])

    assert len(joined_table.column_names) == 6
    # test against pandas' inner join
    df_left = pd.DataFrame(data_car_left, columns=header_car_left)
    df_right = pd.DataFrame(data_car_right, columns=header_car_right)
    df_joined = df_left.merge(df_right, how="inner", on=["CarName", "ModelYear"])
    assert joined_table.data == df_joined.values.tolist()

def test_perform_full_outer_join():
    # single attribute key
    table_left = MyPyTable(header_left, data_left)
    table_right = MyPyTable(header_right, data_right)
    joined_table = table_left.perform_full_outer_join(table_right, ["Product"])

    assert len(joined_table.column_names) == 3
    # test against pandas' outer join
    df_left = pd.DataFrame(data_left, columns=header_left)
    df_right = pd.DataFrame(data_right, columns=header_right)
    df_joined = df_left.merge(df_right, how="outer", on=["Product"])
    df_joined.fillna("NA", inplace=True)
    assert joined_table.data == df_joined.values.tolist()

    # multiple attirbute key example from class
    table_left = MyPyTable(header_car_left, data_car_left)
    table_right = MyPyTable(header_car_right, data_car_right)
    joined_table = table_left.perform_full_outer_join(table_right, ["CarName", "ModelYear"])

    assert len(joined_table.column_names) == 6
    # test against pandas' outer join
    df_left = pd.DataFrame(data_car_left, columns=header_car_left)
    df_right = pd.DataFrame(data_car_right, columns=header_car_right)
    df_joined = df_left.merge(df_right, how="outer", on=["CarName", "ModelYear"])
    df_joined.fillna("NA", inplace=True)
    assert joined_table.data == df_joined.values.tolist()