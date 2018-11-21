from sciqlopcache.amda import AMDA, extract_header
import unittest
from datetime import datetime


class AMDATest(unittest.TestCase):
    def setUp(self):
        self.amda = AMDA()

    def tearDown(self):
        pass

    def test_get_simple_product_rest(self):
        startdate = datetime(2006, 1, 8, 1, 0, 0)
        stopdate = datetime(2006, 1, 8, 1, 1, 0)
        parameterID = "c1_b_gsm"
        c1_b_5vps = self.amda.get_parameter(start_time=startdate, stop_time=stopdate, parameter_id=parameterID, method="REST")
        self.assertTrue(len(c1_b_5vps) == 15)

    def test_extract_header(self):
        sample_header='''
# -----------
# AMDA INFO :
# -----------
# AMDA_ABOUT : Created by AMDA(c)
# AMDA_VERSION : 3.6.0
# AMDA_ACKNOWLEDGEMENT : CDPP/AMDA Team
#
# --------------
# REQUEST INFO :
# --------------
# REQUEST_STRUCTURE : one-file-per-parameter-per-interval
# REQUEST_TIME_FORMAT : ISO 8601
# REQUEST_OUTPUT_PARAMS : c1_b_gsm
#
# --------------------
# DERIVED PARAMETERS :
# --------------------
#
# PARAMETER_ID : framesTransformation_3687406037078806408
# PARAMETER_NAME : c1_b_gse
# PARAMETER_SHORT_NAME : b_gse
# PARAMETER_PROCESS_INFO : Single param process from 'clust1_fgm_prp_FGM'
# PARAMETER_LINKED_PARAMS : clust1_fgm_prp_FGM
#
#
# PARAMETER_ID : c1_b_gsm
# PARAMETER_NAME : c1_b_gsm
# PARAMETER_SHORT_NAME : b_gsm
# PARAMETER_PROCESS_INFO : Derived parameter from expression '#framesTransformation($c1_b_gse;GSE;GSM)'
# PARAMETER_PROCESS_DESC : #framesTransformation($c1_b_gse;GSE;GSM)
# PARAMETER_LINKED_PARAMS : framesTransformation_3687406037078806408
#
#
# -----------------
# BASE PARAMETERS :
# -----------------
#
# MISSION_ID : Cluster1
# MISSION_NAME : Cluster 1
# MISSION_DESCRIPTION : Rumba
# MISSION_URL : http://sci.esa.int/jump.cfm?oid=47348
#
#   INSTRUMENT_ID : Cluster1_fgm
#   INSTRUMENT_NAME : FGM
#   INSTRUMENT_DESCRIPTION : Fluxgate Magnetometer
#   INSTRUMENT_PI : spase://SMWG/Person/Chris.Carr
#   INSTRUMENT_TYPE : Magnetometer
#
#     DATASET_ID : clust1-fgm-prp
#     DATASET_NAME : 4 sec (spin)
#     DATASET_DESCRIPTION : Cluster 1 Spin Resolution FGM Data
#     DATASET_SOURCE : CDPP/DDServer
#     DATASET_GLOBAL_START : 2001-01-07T05:14:29.169
#     DATASET_GLOBAL_STOP : 2018-05-01T07:03:55.374
#     DATASET_MIN_SAMPLING : 4
#     DATASET_MAX_SAMPLING : 4
#     DATASET_ACKNOWLEDGEMENT : Principal Investigator: Chris Carr
#
#       PARAMETER_ID : c1_b_gse
#       PARAMETER_NAME : c1_b_gse
#       PARAMETER_SHORT_NAME : b_gse
#       PARAMETER_COMPONENTS : bx,by,bz
#       PARAMETER_UNITS : nT
#       PARAMETER_COORDINATE_SYSTEM : GSE
#       PARAMETER_TENSOR_ORDER : 0
#       PARAMETER_SI_CONVERSION : 1e-9>T
#       PARAMETER_TABLE : None
#       PARAMETER_FILL_VALUE : nan
#       PARAMETER_UCD : phys.magField
#
#
# ---------------
# INTERVAL INFO :
# ---------------
# INTERVAL_START : 2006-01-08T01:00:00.000
# INTERVAL_STOP : 2006-01-08T01:01:00.000
#
# ------
# DATA :
# ------
# DATA_COLUMNS : AMDA_TIME, c1_b_gsm[0], c1_b_gsm[1], c1_b_gsm[2]
#
        '''
        expected_header = '''
# -----------
# AMDA INFO :
# -----------
# AMDA_ABOUT : Created by AMDA(c)
# AMDA_VERSION : 3.6.0
# AMDA_ACKNOWLEDGEMENT : CDPP/AMDA Team
#
# --------------
# REQUEST INFO :
# --------------
# REQUEST_STRUCTURE : one-file-per-parameter-per-interval
# REQUEST_TIME_FORMAT : ISO 8601
# REQUEST_OUTPUT_PARAMS : c1_b_gsm
#
# --------------------
# DERIVED PARAMETERS :
# --------------------
#
# PARAMETER_ID : framesTransformation_3687406037078806408
# PARAMETER_NAME : c1_b_gse
# PARAMETER_SHORT_NAME : b_gse
# PARAMETER_PROCESS_INFO : Single param process from 'clust1_fgm_prp_FGM'
# PARAMETER_LINKED_PARAMS : clust1_fgm_prp_FGM
#
#
# PARAMETER_ID : c1_b_gsm
# PARAMETER_NAME : c1_b_gsm
# PARAMETER_SHORT_NAME : b_gsm
# PARAMETER_PROCESS_INFO : Derived parameter from expression '#framesTransformation($c1_b_gse;GSE;GSM)'
# PARAMETER_PROCESS_DESC : #framesTransformation($c1_b_gse;GSE;GSM)
# PARAMETER_LINKED_PARAMS : framesTransformation_3687406037078806408
#
#
# -----------------
# BASE PARAMETERS :
# -----------------
#
# MISSION_ID : Cluster1
# MISSION_NAME : Cluster 1
# MISSION_DESCRIPTION : Rumba
# MISSION_URL : http://sci.esa.int/jump.cfm?oid=47348
#
#   INSTRUMENT_ID : Cluster1_fgm
#   INSTRUMENT_NAME : FGM
#   INSTRUMENT_DESCRIPTION : Fluxgate Magnetometer
#   INSTRUMENT_PI : spase://SMWG/Person/Chris.Carr
#   INSTRUMENT_TYPE : Magnetometer
#
#     DATASET_ID : clust1-fgm-prp
#     DATASET_NAME : 4 sec (spin)
#     DATASET_DESCRIPTION : Cluster 1 Spin Resolution FGM Data
#     DATASET_SOURCE : CDPP/DDServer
#     DATASET_GLOBAL_START : 2001-01-07T05:14:29.169
#     DATASET_GLOBAL_STOP : 2018-05-01T07:03:55.374
#     DATASET_MIN_SAMPLING : 4
#     DATASET_MAX_SAMPLING : 4
#     DATASET_ACKNOWLEDGEMENT : Principal Investigator: Chris Carr
#
#       PARAMETER_ID : c1_b_gse
#       PARAMETER_NAME : c1_b_gse
#       PARAMETER_SHORT_NAME : b_gse
#       PARAMETER_COMPONENTS : bx,by,bz
#       PARAMETER_UNITS : nT
#       PARAMETER_COORDINATE_SYSTEM : GSE
#       PARAMETER_TENSOR_ORDER : 0
#       PARAMETER_SI_CONVERSION : 1e-9>T
#       PARAMETER_TABLE : None
#       PARAMETER_FILL_VALUE : nan
#       PARAMETER_UCD : phys.magField
#
#
# ---------------
# INTERVAL INFO :
# ---------------
# INTERVAL_START : {interval_start}
# INTERVAL_STOP : {interval_stop}
#
# ------
# DATA :
# ------
# DATA_COLUMNS : AMDA_TIME, c1_b_gsm[0], c1_b_gsm[1], c1_b_gsm[2]
#
        '''
        result = extract_header(sample_header)

    def test_add_to_cache(self):
        pass