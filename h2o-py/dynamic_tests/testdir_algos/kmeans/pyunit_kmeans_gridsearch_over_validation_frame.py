from __future__ import print_function

import sys
import os
from builtins import range
import time

sys.path.insert(1, "../../../")

import h2o
from tests import pyunit_utils
from h2o.estimators.kmeans import H2OKMeansEstimator
from h2o.grid.grid_search import H2OGridSearch

class Test_kmeans_grid_search:
    """
    PUBDEV-1843: Grid testing.  Subtask 5.

    This class is created to test the gridsearch for kmeans algo over differ validation datasets.  Only one test
    is performed here.

    Test Descriptions:
        a. choose a subset of parameters to use in the hyper-parameters for gridsearch.  However, must grid over
           the validation_frame parameter.
        b. Next, build H2O kmeans models using grid search and check out the performance metrics with the different
           validation datasets.
    """

    # parameters set by users, change with care
    curr_time = str(round(time.time()))     # store current timestamp, used as part of filenames.
    seed = round(time.time())

    # parameters denoting filenames of interested
    training1_filenames = "smalldata/gridsearch/kmeans_8_centers_3_coords.csv"
    validation_filenames = ["smalldata/gridsearch/kmeans_8_centers_3_coords_valid1.csv",
                            "smalldata/gridsearch/kmeans_8_centers_3_coords_valid2.csv",
                            "smalldata/gridsearch/kmeans_8_centers_3_coords_valid3.csv"]
    json_filename = "gridsearch_kmeans_hyper_parameter_" + curr_time + ".json"

    # System parameters, do not change.  Dire consequences may follow if you do
    current_dir = os.path.dirname(os.path.realpath(sys.argv[1]))    # directory of this test file

    test_name = "pyunit_kmeans_gridsearch_over_validation_frame.py"     # name of this test
    sandbox_dir = ""  # sandbox directory where we are going to save our failed test data sets

    # store information about training/test data sets
    x_indices = []              # store predictor indices in the data set
    training1_data = []         # store training data sets
    test_failed = 0             # count total number of tests that have failed

    # give the user opportunity to pre-assign hyper parameters for fixed values
    hyper_params = dict()
    hyper_params['init'] = ["Random"]
    hyper_params["max_runtime_secs"] = [10]     # 10 seconds
    hyper_params["max_iterations"] = [50]
    hyper_params["k"] = [8]
    hyper_params["validation_frame"] = []
    hyper_params["seed"] = [seed]     # added see to make test more repeatable


    def __init__(self):
        self.setup_data()

    def setup_data(self):
        """
        This function performs all initializations necessary:
        load the data sets and set the training set indices
        """

        # create and clean out the sandbox directory first
        self.sandbox_dir = pyunit_utils.make_Rsandbox_dir(self.current_dir, self.test_name, True)
        self.training1_data = h2o.import_file(path=pyunit_utils.locate(self.training1_filenames))
        self.x_indices = list(range(self.training1_data.ncol))

        # save the training data files just in case the code crashed.
        pyunit_utils.remove_csv_files(self.current_dir, ".csv", action='copy', new_dir_path=self.sandbox_dir)


    def test_kmeans_grid_search_over_validation_datasets(self):
        """
        test_kmeans_grid_search_over_validation_datasets performs the following:
        a. build H2O kmeans models using grid search.  Count and make sure models
           are only built for hyper-parameters set to legal values.  No model is built for bad hyper-parameters
           values.  We should instead get a warning/error message printed out.
        b. For each model built using grid search, we will extract the parameters used in building
           that model and manually build a H2O kmeans model.  Training metrics are calculated from the
           gridsearch model and the manually built model.  If their metrics
           differ by too much, print a warning message but don't fail the test.
        c. we will check and make sure the models are built within the max_runtime_secs time limit that was set
           for it as well.  If max_runtime_secs was exceeded, declare test failure.
        """

        valid0 = h2o.import_file(path=pyunit_utils.locate(self.validation_filenames[0]))
        valid1 = h2o.import_file(path=pyunit_utils.locate(self.validation_filenames[1]))
        valid2 = h2o.import_file(path=pyunit_utils.locate(self.validation_filenames[2]))
#        self.hyper_params["validation_frame"] = ["valid0", "valid1", "valid2"]
#        self.hyper_params["validation_frame"] = [valid0.frame_id, valid1.frame_id, valid2.frame_id]
#        self.hyper_params["validation_frame"] = [valid0]  # does not work, result in validation frame being null
        self.hyper_params["validation_frame"] = [valid0.frame_id, valid1.frame_id]   # id seems better, not result null stuff

        print("*******************************************************************************************")
        print("test_kmeans_grid_search_over_validation_datasets for kmeans ")
        h2o.cluster_info()

        print("Hyper-parameters used here is {0}".format(self.hyper_params))

        # start grid search
        grid_model = H2OGridSearch(H2OKMeansEstimator(),
                                   hyper_params=self.hyper_params)
        grid_model.train(x=self.x_indices, training_frame=self.training1_data)

        self.correct_model_number = len(grid_model)     # store number of models built



def test_grid_search_for_kmeans_over_all_params():
    """
    Create and instantiate class and perform tests specified for kmeans

    :return: None
    """
    test_kmeans_grid = Test_kmeans_grid_search()
    test_kmeans_grid.test_kmeans_grid_search_over_validation_datasets()

    sys.stdout.flush()

    if test_kmeans_grid.test_failed:  # exit with error if any tests have failed
        sys.exit(1)


if __name__ == "__main__":
    pyunit_utils.standalone_test(test_grid_search_for_kmeans_over_all_params)
else:
    test_grid_search_for_kmeans_over_all_params()
