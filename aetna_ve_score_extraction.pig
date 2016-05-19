SET job.name 'Aetna_DRG_Value_Estimation_Score'
SET pig.logfile 'Aetna_DRG_Value_Estimation_Score.log'
SET mapred.child.java.opts -Xmx1g

REGISTER /opt/cloudera/parcels/CDH/lib/pig/piggybank.jar;
REGISTER /opt/cloudera/parcels/CDH/lib/pig/datafu-1.1.0-cdh5.4.7.jar
REGISTER 'DRG_Val_Est_Pay_Pred.py' using jython;

define dateFormat com.iht.pig.udfs.common.DateFormatConverter();
define dateDiff com.iht.pig.udfs.common.DateDiff(); 
define VAR datafu.pig.stats.VAR();
define Median datafu.pig.stats.StreamingMedian();

%default date_stamp `date +%Y-%m-%d`
%default project 'Aetna'
%default payer 'HMO' --'HMO''Traditional'
%default dflt_drg_type '_MS-DRG'

%declare icddiag10to9_path '/user/johnbrusk/icd10to9'
%declare icdproc10to9_path '/user/johnbrusk/icd10to9proc'
%declare DENORM_PATH '/refined/analytics/$project/$payer/Denorms/Claims'
%declare ADMIT_DATA_PATH '/user/johnbrusk/AE_CH_04_22_16'
%declare DRG_MASTER_PATH '/refined/icrs_rules/drg_code_master'
%declare DRG_VALUE_ESTMATION_TRAIN_SET_OUT_PATH '/tmp/jbrusk/DRG_VALUE_EST_TRAIN_SET_$date_stamp'
%declare DRG_VALUE_ESTMATION_SCORE_SET_OUT_PATH '/tmp/jbrusk/DRG_VALUE_EST_SCORE_SET_$date_stamp'


%declare DENORM_SCHEMA '(PAYER: chararray
,CLAIM_TYPE: chararray
,CLAIM_ID: chararray
,BASE_CLAIM_ID: chararray
,Adj_Seq: int
,LINE_SEQ: int
,CLAIM_DOS_FROM: chararray
,CLAIM_DOS_TO: chararray
,ADMIT_DATE: chararray
,ADMIT_HOUR: chararray
,ADMIT_MINUTE: chararray
,DISCHARGE_DATE: chararray
,DISCHARGE_HOUR: chararray
,DISCHARGE_MINUTE: chararray
,ACTUAL_LENGTH_OF_STAY: int
,DOS_FROM: chararray
,DOS_TO: chararray
,LINE_DOS: chararray
,INSURANCE_ID: chararray
,INSURANCE_ID_DESC: chararray
,INSURANCE_LOB: chararray
,INSURANCE_LOB_DESC: chararray
,INSURANCE_PRODUCT: chararray
,INSURANCE_PRODUCT_DESC: chararray
,INSURANCE_PLAN_CODE: chararray
,INSURANCE_PLAN_DESC: chararray
,MEMBER_ID: chararray
,MEMBER_ID_UNIQUE: chararray
,SUB_ID: chararray
,DEP_ID: chararray
,DOB: chararray
,GENDER_ID: chararray
,PATIENT_ACCOUNT_NUMBER: chararray
,PROVIDER_ID: chararray
,SUBSPEC_ID: chararray
,POS_ID: chararray
,POS_ID_DESC: chararray
,BILL_TYPE: chararray
,SUB_DRG: chararray
,SUB_SEVERITY: chararray
,ALLOWED_DRG: chararray
,ALLOWED_SEVERITY: chararray
,ADMIT_TYPE: chararray
,ADMIT_SOURCE: chararray
,DISCHARGE_STATUS: chararray
,APC_CODE: chararray
,APC_VERSION: chararray
,APC_PAYMENT_WEIGHT: float
,APG_CODE: chararray
,APG_VERSION: chararray
,SUB_REV_CODE: chararray
,SUB_CPT: chararray
,SUB_MOD1: chararray
,SUB_MOD2: chararray
,SUB_MOD3: chararray
,SUB_MOD4: chararray
,SUB_UNITS: float
,SUB_AMOUNT: double
,REV_CODE: chararray
,CPT: chararray
,MOD1: chararray
,MOD2: chararray
,MOD3: chararray
,MOD4: chararray
,UNITS: float
,AMOUNT: double
,COPAY: double
,COINSURANCE: double
,DEDUCTIBLE: double
,COB: double
,OTHER_REDUCTION: double
,OUTLIER_AMOUNT: double
,PAID: double
,PAID_DATE: chararray
,PAR_CODE: chararray
,PAR_CODE_DESC: chararray
,CLAIM_DIAG_1: chararray
,CLAIM_DIAG_1_QUAL: chararray
,CLAIM_DIAG_2: chararray
,CLAIM_DIAG_2_QUAL: chararray
,CLAIM_DIAG_3: chararray
,CLAIM_DIAG_3_QUAL: chararray
,CLAIM_DIAG_4: chararray
,CLAIM_DIAG_4_QUAL: chararray
,CLAIM_DIAG_5: chararray
,CLAIM_DIAG_5_QUAL: chararray
,CLAIM_DIAG_6: chararray
,CLAIM_DIAG_6_QUAL: chararray
,CLAIM_DIAG_7: chararray
,CLAIM_DIAG_7_QUAL: chararray
,CLAIM_DIAG_8: chararray
,CLAIM_DIAG_8_QUAL: chararray
,CLAIM_DIAG_9: chararray
,CLAIM_DIAG_9_QUAL: chararray
,CLAIM_DIAG_10: chararray
,CLAIM_DIAG_10_QUAL: chararray
,CLAIM_DIAG_11: chararray
,CLAIM_DIAG_11_QUAL: chararray
,CLAIM_DIAG_12: chararray
,CLAIM_DIAG_12_QUAL: chararray
,ADMIT_DIAG: chararray
,ADMIT_DIAG_QUAL: chararray
,BILLING_PROVIDER_ID: chararray
,OPERATING_PROVIDER_ID: chararray
,PRESCRIBING_PROVIDER_ID: chararray
,ATTENDING_PROVIDER_ID: chararray
,REFERRING_PROVIDER_ID: chararray
,Claim_SUB_UNITS: float
,Claim_SUB_AMOUNT: double
,Claim_UNITS: float
,Claim_AMOUNT: double
,Claim_COPAY: double
,Claim_COINSURANCE: double
,Claim_DEDUCTIBLE: double
,Claim_COB: double
,Claim_OTHER_REDUCTION: double
,Claim_OUTLIER_AMOUNT: double
,Claim_PAID: double
,Claim_PAID_DATE: chararray
,PAYEE_CODE: chararray
,Claim_DATE_RECEIVE_CLIENT: chararray
,Claim_AUTHORIZATION_CODE: chararray
,CHECK_NUMBER: chararray
,DATE_ADJUDICATED: chararray
,NDC_CODE: chararray
,ADJUSTMENT_INDICATOR: chararray
,WHOLE_CLAIM_PRICING_LINE_YN: chararray
,ANESTHESIA_TIME: chararray
,ICD_PROC_1: chararray
,ICD_PROC_DATE_1: chararray
,ICD_PROC_1_QUAL: chararray
,ICD_PROC_2: chararray
,ICD_PROC_DATE_2: chararray
,ICD_PROC_2_QUAL: chararray
,ICD_PROC_3: chararray
,ICD_PROC_DATE_3: chararray
,ICD_PROC_3_QUAL: chararray
,ICD_PROC_4: chararray
,ICD_PROC_DATE_4: chararray
,ICD_PROC_4_QUAL: chararray
,ICD_PROC_5: chararray
,ICD_PROC_DATE_5: chararray
,ICD_PROC_5_QUAL: chararray
,ICD_PROC_6: chararray
,ICD_PROC_DATE_6: chararray
,ICD_PROC_6_QUAL: chararray
,LINE_DIAG_1: chararray
,LINE_DIAG_1_QUAL: chararray
,LINE_DIAG_2: chararray
,LINE_DIAG_2_QUAL: chararray
,LINE_DIAG_3: chararray
,LINE_DIAG_3_QUAL: chararray
,LINE_DIAG_4: chararray
,LINE_DIAG_4_QUAL: chararray
,CAPITATION_INDICATOR: chararray
,DATE_RECEIVE_CLIENT: chararray
,NDC_CODE_DESC: chararray
,DRUG_NAME: chararray
,DAYS_SUPPLY: chararray
,RX_WRITTEN_DATE: chararray
,RX_NUMBER: chararray
,REFILL_NUMBER: chararray
,DOSAGE_FORM: chararray
,PACKAGE_SIZE: chararray
,PACKAGE_QUANTITY: chararray
,FORMULARY_INDICATOR: chararray
,BRAND_IND: chararray
,BRAND_DESC: chararray
,DISPENSING_FEE: double
,THERAPY_CLASS_CODE: chararray
,THERAPY_CLASS_DESC: chararray
,MAIL_ORDER_INDICATOR: chararray
,GENERIC_INDICATOR: chararray
,THEURAPUTIC_CODE: chararray
,THEURAPTUC_CODE_DESC: chararray
,DISPENSE_AS_WRITTEN_CODE: chararray
,DISPENSE_AS_WRITTEN_DESC: chararray
,LINE_SEQ_ORIG: int
,ADJUSTMENT_SEQ: int
,TOS_CODE: chararray
,AUTHORIZATION_CODE: chararray
,CLAIMS_STATUS_CODE: chararray
,Specimen_Type: chararray
,SpecimenID: chararray
,Diagnostic_Code1: chararray
,Ordered_Test_Number: chararray
,Result_Test_Number: chararray
,Test_Name: chararray
,Normal_Low: chararray
,Normal_High: chararray
,Result: chararray
,Abnormal_Code: chararray
,LO_ID: chararray
,Ordering_Physician_Name: chararray
,Ordering_Physician_Upin_Code: chararray
,Lab_Code: chararray
,Specimen_Date: chararray
,Result_Units: chararray
,Service_place: chararray
,Result_text: chararray
,PAYMENT_METHODOLOGY: chararray
,Claim_DATE_ADJUDICATED: chararray
,DEA_NUMBER: chararray
,BLUE_CARD_HOME_HOST_INDICATOR: chararray
,COVERED_LENGTH_OF_STAY: chararray
,BIRTH_WEIGHT: chararray
,GROUPER_ID: chararray
,GROUPER_TYPE: chararray
,GROUPER_VERSION: chararray
,PRIMARY_PAYER_FLAG: chararray
,CLAIM_ID_ORIG: chararray
,Claim_ADJUSTMENT_INDICATOR: chararray
,Claim_ADJUSTMENT_SEQ: chararray
,Claim_CLAIMS_STATUS_CODE: chararray
,GROUP_ID: chararray
,PARTNER_ID: chararray
,FACILITY_TYPE: chararray
,COUNTY_CODE: chararray
,NURSERY_LEVEL: chararray
,PSYCH_UNIT_INDICATOR: chararray
,ALTERNATE_LEVEL_OF_CARE_DAYS: chararray
,RETIREE_INDICATOR: chararray
,Ordering_Provider_ID: chararray
,LAB_Provider_ID: chararray
,LAB_NPI: chararray
,CLAIM_DIAG_13: chararray
,CLAIM_DIAG_13_QUAL: chararray
,CLAIM_DIAG_14: chararray
,CLAIM_DIAG_14_QUAL: chararray
,CLAIM_DIAG_15: chararray
,CLAIM_DIAG_15_QUAL: chararray
,CLAIM_DIAG_16: chararray
,CLAIM_DIAG_16_QUAL: chararray
,CLAIM_DIAG_17: chararray
,CLAIM_DIAG_17_QUAL: chararray
,CLAIM_DIAG_18: chararray
,CLAIM_DIAG_18_QUAL: chararray
,CLAIM_DIAG_19: chararray
,CLAIM_DIAG_19_QUAL: chararray
,CLAIM_DIAG_20: chararray
,CLAIM_DIAG_20_QUAL: chararray
,CLAIM_DIAG_21: chararray
,CLAIM_DIAG_21_QUAL: chararray
,CLAIM_DIAG_22: chararray
,CLAIM_DIAG_22_QUAL: chararray
,CLAIM_DIAG_23: chararray
,CLAIM_DIAG_23_QUAL: chararray
,CLAIM_DIAG_24: chararray
,CLAIM_DIAG_24_QUAL: chararray
,CLAIM_DIAG_25: chararray
,CLAIM_DIAG_25_QUAL: chararray
,E_CODE_DIAG_1: chararray
,E_CODE_DIAG_1_QUAL: chararray
,E_CODE_DIAG_2: chararray
,E_CODE_DIAG_2_QUAL: chararray
,E_CODE_DIAG_3: chararray
,E_CODE_DIAG_3_QUAL: chararray
,E_CODE_DIAG_4: chararray
,E_CODE_DIAG_4_QUAL: chararray
,E_CODE_DIAG_5: chararray
,E_CODE_DIAG_5_QUAL: chararray
,E_CODE_DIAG_6: chararray
,E_CODE_DIAG_6_QUAL: chararray
,E_CODE_DIAG_7: chararray
,E_CODE_DIAG_7_QUAL: chararray
,E_CODE_DIAG_8: chararray
,E_CODE_DIAG_8_QUAL: chararray
,E_CODE_DIAG_9: chararray
,E_CODE_DIAG_9_QUAL: chararray
,E_CODE_DIAG_10: chararray
,E_CODE_DIAG_10_QUAL: chararray
,E_CODE_DIAG_11: chararray
,E_CODE_DIAG_11_QUAL: chararray
,E_CODE_DIAG_12: chararray
,E_CODE_DIAG_12_QUAL: chararray
,RFV_DIAG_1: chararray
,RFV_DIAG_1_QUAL: chararray
,RFV_DIAG_2: chararray
,RFV_DIAG_2_QUAL: chararray
,RFV_DIAG_3: chararray
,RFV_DIAG_3_QUAL: chararray
,CLAIM_DIAG_POA_1: chararray
,CLAIM_DIAG_POA_2: chararray
,CLAIM_DIAG_POA_3: chararray
,CLAIM_DIAG_POA_4: chararray
,CLAIM_DIAG_POA_5: chararray
,CLAIM_DIAG_POA_6: chararray
,CLAIM_DIAG_POA_7: chararray
,CLAIM_DIAG_POA_8: chararray
,CLAIM_DIAG_POA_9: chararray
,CLAIM_DIAG_POA_10: chararray
,CLAIM_DIAG_POA_11: chararray
,CLAIM_DIAG_POA_12: chararray
,CLAIM_DIAG_POA_13: chararray
,CLAIM_DIAG_POA_14: chararray
,CLAIM_DIAG_POA_15: chararray
,CLAIM_DIAG_POA_16: chararray
,CLAIM_DIAG_POA_17: chararray
,CLAIM_DIAG_POA_18: chararray
,CLAIM_DIAG_POA_19: chararray
,CLAIM_DIAG_POA_20: chararray
,CLAIM_DIAG_POA_21: chararray
,CLAIM_DIAG_POA_22: chararray
,CLAIM_DIAG_POA_23: chararray
,CLAIM_DIAG_POA_24: chararray
,CLAIM_DIAG_POA_25: chararray
,COND_CODE_1: chararray
,COND_CODE_2: chararray
,COND_CODE_3: chararray
,COND_CODE_4: chararray
,COND_CODE_5: chararray
,COND_CODE_6: chararray
,COND_CODE_7: chararray
,COND_CODE_8: chararray
,COND_CODE_9: chararray
,COND_CODE_10: chararray
,COND_CODE_11: chararray
,COND_CODE_12: chararray
,COND_CODE_13: chararray
,COND_CODE_14: chararray
,COND_CODE_15: chararray
,COND_CODE_16: chararray
,COND_CODE_17: chararray
,COND_CODE_18: chararray
,COND_CODE_19: chararray
,COND_CODE_20: chararray
,COND_CODE_21: chararray
,COND_CODE_22: chararray
,COND_CODE_23: chararray
,COND_CODE_24: chararray
,VALUE_CODE_1: chararray
,VALUE_CODE_2: chararray
,VALUE_CODE_3: chararray
,VALUE_CODE_4: chararray
,VALUE_CODE_5: chararray
,VALUE_CODE_6: chararray
,VALUE_CODE_7: chararray
,VALUE_CODE_8: chararray
,VALUE_CODE_9: chararray
,VALUE_CODE_10: chararray
,VALUE_CODE_11: chararray
,VALUE_CODE_12: chararray
,VALUE_CODE_13: chararray
,VALUE_CODE_14: chararray
,VALUE_CODE_15: chararray
,VALUE_CODE_16: chararray
,VALUE_CODE_17: chararray
,VALUE_CODE_18: chararray
,VALUE_CODE_19: chararray
,VALUE_CODE_20: chararray
,VALUE_CODE_21: chararray
,VALUE_CODE_22: chararray
,VALUE_CODE_23: chararray
,VALUE_CODE_24: chararray
,VALUE_AMOUNT_1: double
,VALUE_AMOUNT_2: double
,VALUE_AMOUNT_3: double
,VALUE_AMOUNT_4: double
,VALUE_AMOUNT_5: double
,VALUE_AMOUNT_6: double
,VALUE_AMOUNT_7: double
,VALUE_AMOUNT_8: double
,VALUE_AMOUNT_9: double
,VALUE_AMOUNT_10: double
,VALUE_AMOUNT_11: double
,VALUE_AMOUNT_12: double
,VALUE_AMOUNT_13: double
,VALUE_AMOUNT_14: double
,VALUE_AMOUNT_15: double
,VALUE_AMOUNT_16: double
,VALUE_AMOUNT_17: double
,VALUE_AMOUNT_18: double
,VALUE_AMOUNT_19: double
,VALUE_AMOUNT_20: double
,VALUE_AMOUNT_21: double
,VALUE_AMOUNT_22: double
,VALUE_AMOUNT_23: double
,VALUE_AMOUNT_24: double
,ICD_PROC_7: chararray
,ICD_PROC_DATE_7: chararray
,ICD_PROC_7_QUAL: chararray
,ICD_PROC_8: chararray
,ICD_PROC_DATE_8: chararray
,ICD_PROC_8_QUAL: chararray
,ICD_PROC_9: chararray
,ICD_PROC_DATE_9: chararray
,ICD_PROC_9_QUAL: chararray
,ICD_PROC_10: chararray
,ICD_PROC_DATE_10: chararray
,ICD_PROC_10_QUAL: chararray
,ICD_PROC_11: chararray
,ICD_PROC_DATE_11: chararray
,ICD_PROC_11_QUAL: chararray
,ICD_PROC_12: chararray
,ICD_PROC_DATE_12: chararray
,ICD_PROC_12_QUAL: chararray
,ICD_PROC_13: chararray
,ICD_PROC_DATE_13: chararray
,ICD_PROC_13_QUAL: chararray
,ICD_PROC_14: chararray
,ICD_PROC_DATE_14: chararray
,ICD_PROC_14_QUAL: chararray
,ICD_PROC_15: chararray
,ICD_PROC_DATE_15: chararray
,ICD_PROC_15_QUAL: chararray
,ICD_PROC_16: chararray
,ICD_PROC_DATE_16: chararray
,ICD_PROC_16_QUAL: chararray
,ICD_PROC_17: chararray
,ICD_PROC_DATE_17: chararray
,ICD_PROC_17_QUAL: chararray
,ICD_PROC_18: chararray
,ICD_PROC_DATE_18: chararray
,ICD_PROC_18_QUAL: chararray
,ICD_PROC_19: chararray
,ICD_PROC_DATE_19: chararray
,ICD_PROC_19_QUAL: chararray
,ICD_PROC_20: chararray
,ICD_PROC_DATE_20: chararray
,ICD_PROC_20_QUAL: chararray
,ICD_PROC_21: chararray
,ICD_PROC_DATE_21: chararray
,ICD_PROC_21_QUAL: chararray
,ICD_PROC_22: chararray
,ICD_PROC_DATE_22: chararray
,ICD_PROC_22_QUAL: chararray
,ICD_PROC_23: chararray
,ICD_PROC_DATE_23: chararray
,ICD_PROC_23_QUAL: chararray
,ICD_PROC_24: chararray
,ICD_PROC_DATE_24: chararray
,ICD_PROC_24_QUAL: chararray
,ICD_PROC_25: chararray
,ICD_PROC_DATE_25: chararray
,ICD_PROC_25_QUAL: chararray
,ASSIGNMENT_OF_BENEFITS: chararray
,NPI_ATTENDING: chararray
,Claim_CHECK_NUMBER: chararray
,CONTRACT: chararray
,MED_REC_NO: chararray
,Mem_DEPENDENT_FIRST_NAME: chararray
,Mem_DEPENDENT_LAST_NAME: chararray
,Mem_DEPENDENT_SSN: chararray
,Mem_DEPENDENT_STATE: chararray
,Mem_DEPENDENT_ZIP: chararray
,Mem_DEPENDENT_RELATIONSHIP_CODE: chararray
,Mem_DEPENDENT_RELATIONSHIP_DESC: chararray
,Mem_GROUP_ID: chararray
,Mem_GROUP_DESC: chararray
,Mem_EFFECTIVE_DATE: chararray
,Mem_TERM_DATE: chararray
,DOS_Btw_Eff_Dates_YN: chararray
,Mem_LOB: chararray
,Mem_PRODUCT: chararray
,Mem_PRODUCT_NAME: chararray
,Mem_SUBSCRIBER_FIRST_NAME: chararray
,Mem_SUBSCRIBER_LAST_NAME: chararray
,Mem_SUBSCRIBER_DOB: chararray
,Mem_SUBSCRIBER_GENDER: chararray
,Mem_SUBSCRIBER_SSN: chararray
,Mem_MEDICARE_NUMBER: chararray
,Mem_MEDICAID_NUMBER: chararray
,Mem_DEPENDENT_PREFIX: chararray
,Mem_DEPENDENT_MIDDLE_NAME: chararray
,Mem_DEPENDENT_SUFFIX: chararray
,Mem_DEPENDENT_ADDRESS_1: chararray
,Mem_DEPENDENT_ADDRESS_2: chararray
,Mem_DEPENDENT_CITY: chararray
,Mem_SUB_GROUP_ID: chararray
,Mem_SUB_GROUP_DESC: chararray
,Mem_ASO_IND: chararray
,Mem_PRODUCT_OPTION: chararray
,Mem_PRODUCT_RATING_LEVEL: chararray
,Mem_SUBSCRIBER_PREFIX: chararray
,Mem_SUBSCRIBER_MIDDLE_NAME: chararray
,Mem_SUBSCRIBER_SUFFIX: chararray
,Mem_COBRA_INDICATOR: chararray
,Mem_COB_INDICATOR: chararray
,Mem_MAJOR_RISK_CLASS_CODE: chararray
,Mem_MAJOR_RISK_CLASS_DESC: chararray
,Mem_EMPLOYER_ID: chararray
,Mem_MEMBER_PCP_ID: chararray
,Mem_FUNDING_ARRANGEMENT: chararray
,Mem_RISK_CAT_NAME: chararray
,Mem_LARGE_SMALL_GROUP_INDICATOR: chararray
,Mem_RATING_REGION: chararray
,Mem_RISK_SCORE: chararray
,Mem_MEMBER_DATE_OF_DEATH: chararray
,Mem_RETIREE_INDICATOR: chararray
,Prov_PROVIDER_ID: chararray
,Prov_NAME: chararray
,Prov_STREET_ADDRESS_1: chararray
,Prov_STREET_ADDRESS_2: chararray
,Prov_CITY: chararray
,Prov_STATE: chararray
,Prov_COUNTRY: chararray
,Prov_ZIP_CD: chararray
,Prov_ZIP_PLUS4: chararray
,Prov_TAX_ID: chararray
,Prov_NATL_PROV_ID: chararray
,Prov_SPECIALTY_CODE: chararray
,Prov_PROVIDER_TYPE_CODE: chararray
,Prov_TAXONOMY_CODE: chararray
,Prov_PHONE: chararray
,Prov_PHONE_EXT: chararray
,Prov_ALT_PHONE: chararray
,Prov_ALT_PHONE_EXT: chararray
,Prov_FAX: chararray
,Prov_PAR_CODE: chararray
,Prov_PAR_DESC: chararray
,Prov_SPECIALTY_DESC: chararray
,Prov_PROVIDER_TYPE_DESC: chararray
,Prov_COUNTY: chararray
,Prov_HOSPITAL_SYSTEM: chararray
,Prov_REGION: chararray
,Prov_PRODUCT: chararray
,Prov_LOB: chararray
,Prov_CMC_CERTIFICATION_NUMBER: chararray
,ORIG_ALLOWED_AMOUNT: double
,ORIG_PAID: double
,Other_Insurance_Exists_YN: chararray
,OTHER_INSURANCE_CODE: chararray
,OTHER_INSURANCE_CARRIER: chararray
,OTHER_INSURANCE_MEMBER_ID: chararray
,OTHER_INSURANCE_EFFECTIVE_DATE: chararray
,OTHER_INSURANCE_END_DATE: chararray
,MEDICARE_EFFECTIVE_DATE: chararray
,Adjustment_Status: chararray
,Count_Lines: int
,Min_DOS_FROM: chararray
,Max_DOS_FROM: chararray
,Sum_SUBMITTED_UNITS: float
,Sum_SUBMITTED_AMOUNT: double
,Sum_Rev_Rank: int
,Sum_Cpt_Rank: int
,Claim_Adjustment_Link: int
,Join_Claims_Members_10: int
,Join_Claims_Providers_10: int
,Join_Claims_MemberOHI_10: int
,Line_Claim_Type: chararray
,LAST_UPDATE: chararray
)'

%declare DRG_MASTER_SCHEMA '(drg_code_key: chararray, grouper_key: chararray, drg_code: chararray, drg_code_desc: chararray, drg_weight: float, gmlos: float, amlos: float, post_acute_yn: chararray, special_pay_yn: chararray, drg_mdc: chararray, drg_type: chararray, nmlo: int, nmhi: int, soi: chararray, alos: float)'

%declare ADMIT_DATA_SCHEMA '(CnlyClaimNum:chararray, ConceptID:chararray)'


--icd diagnosis codes 10 to 9 file loading and converting to single line
diag_source1 = LOAD '$icddiag10to9_path' USING PigStorage(',') as (icd10:chararray,icd9:chararray);
diag_source = FOREACH diag_source1 GENERATE REPLACE(icd10,'\\u002E','') as icd10, REPLACE(icd9,'\\u002E','') as icd9;
icddiag10to9 = FOREACH (group diag_source BY icd10) GENERATE FLATTEN(group) as icd10, BagToString(diag_source.icd9,'|') as icd9;


--icd procedure codes 10 to 9 file loading and converting to single line
proc_source0 = LOAD '$icdproc10to9_path' USING PigStorage(',') as (icd10:chararray,icd9:chararray);
proc_source = FOREACH proc_source0 GENERATE icd10, icd9;
icdproc10to9 = FOREACH (group proc_source BY icd10) GENERATE FLATTEN(group) as icd10, BagToString(proc_source.icd9,'|') as icd9;


data_in = LOAD '$DENORM_PATH' USING PigStorage('\\u1') AS $DENORM_SCHEMA;

AdminData = LOAD '$ADMIT_DATA_PATH' USING PigStorage(',') AS $ADMIT_DATA_SCHEMA;

AdminDataB = Foreach AdminData Generate CnlyClaimNum, ConceptID;

AdminDataA = Distinct AdminDataB;


JFIX = Foreach data_in Generate CLAIM_ID
--, CONCAT((MEMBER_ID is null ? SUBSTRING(CLAIM_ID,11,22) : MEMBER_ID), CONCAT('-', CONCAT(PROVIDER_ID, CONCAT('-', CONCAT(SUBSTRING(ADMIT_DATE,0,8), '$payer'))))) AS ADMIT_KEY:chararray 
, CONCAT((MEMBER_ID is null ? CLAIM_ID : MEMBER_ID), CONCAT('-', CONCAT(PROVIDER_ID, CONCAT('-', CONCAT(SUBSTRING(ADMIT_DATE,0,8), '$payer'))))) AS ADMIT_KEY:chararray
;

JFIX2 = Distinct JFIX;

JFIX3 = Join JFIX2 by CLAIM_ID, AdminDataA by CnlyClaimNum;  --SUBSTRING(CLAIM_ID,11,22)

JFIX4 = Foreach JFIX3 Generate JFIX2::ADMIT_KEY as ADMIT_KEY, AdminDataA::ConceptID as ConceptID;

JFIX5 = Distinct JFIX4;

data_in2 = Foreach data_in Generate CLAIM_ID, LINE_SEQ, PROVIDER_ID, ADMIT_DATE, DISCHARGE_DATE, DISCHARGE_STATUS, MEMBER_ID_UNIQUE, MEMBER_ID, GENDER_ID, INSURANCE_LOB, DOB, BILL_TYPE, PAR_CODE_DESC, ALLOWED_DRG,
ADMIT_DIAG, CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13,
CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25, ICD_PROC_1, 
ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, ICD_PROC_7, ICD_PROC_8, ICD_PROC_9, ICD_PROC_10, ICD_PROC_11, ICD_PROC_12, ICD_PROC_13, ICD_PROC_14, ICD_PROC_15, ICD_PROC_16,
ICD_PROC_17, ICD_PROC_18, ICD_PROC_19, ICD_PROC_20, ICD_PROC_21, ICD_PROC_22, ICD_PROC_23, ICD_PROC_24, ICD_PROC_25, Claim_UNITS, Claim_SUB_AMOUNT, Claim_AMOUNT, Claim_COPAY, Claim_COINSURANCE, Claim_DEDUCTIBLE, Claim_COB,
Claim_OTHER_REDUCTION, Claim_PAID
--, CONCAT((MEMBER_ID is null ? SUBSTRING(CLAIM_ID,11,22) : MEMBER_ID), CONCAT('-', CONCAT(PROVIDER_ID, CONCAT('-', CONCAT(SUBSTRING(ADMIT_DATE,0,8), '$payer'))))) AS ADMIT_KEY:chararray
, CONCAT((MEMBER_ID is null ? CLAIM_ID : MEMBER_ID), CONCAT('-', CONCAT(PROVIDER_ID, CONCAT('-', CONCAT(SUBSTRING(ADMIT_DATE,0,8), '$payer'))))) AS ADMIT_KEY:chararray
; 

AA = Join data_in2 by ADMIT_KEY, JFIX5 by ADMIT_KEY;

AB1 = Foreach AA Generate '$payer' as PAYER
, data_in2::CLAIM_ID as CLAIM_ID
, JFIX5::ConceptID as ConceptID
, data_in2::LINE_SEQ as LINE_SEQ

--Admit Key for HMO
, CONCAT((MEMBER_ID is null ? CLAIM_ID : MEMBER_ID), CONCAT('-', CONCAT(PROVIDER_ID, CONCAT('-', CONCAT(SUBSTRING(ADMIT_DATE,0,8), '$payer'))))) AS ADMIT_KEY:chararray

--Admit Key for Traditional
--, CONCAT((MEMBER_ID is null ? SUBSTRING(CLAIM_ID,11,22) : MEMBER_ID), CONCAT('-', CONCAT(PROVIDER_ID, CONCAT('-', CONCAT(SUBSTRING(ADMIT_DATE,0,8), '$payer'))))) AS ADMIT_KEY:chararray


, data_in2::PROVIDER_ID as PROVIDER
, SUBSTRING(data_in2::ADMIT_DATE,0,8) as ADMIT_DATE
, SUBSTRING(data_in2::DISCHARGE_DATE,0,8) as DISCHARGE_DATE
, (data_in2::DISCHARGE_STATUS is null ? 'no_data' : data_in2::DISCHARGE_STATUS) as DISCHARGE_STATUS
, (data_in2::MEMBER_ID_UNIQUE is null ? data_in2::MEMBER_ID : data_in2::MEMBER_ID_UNIQUE) AS MEMBER
, (data_in2::GENDER_ID is null ? 'no_data' : (data_in2::GENDER_ID == 'U' ? 'no_data' : data_in2::GENDER_ID)) as GENDER
, (data_in2::INSURANCE_LOB is null ? 'no_data' : data_in2::INSURANCE_LOB ) as LOB
, (data_in2::DOB is null ? 'no_data' : SUBSTRING(data_in2::DOB,0,8)) as DOB
, (data_in2::BILL_TYPE is null ? 'no_data' : data_in2::BILL_TYPE) as BILL_TYPE
, (data_in2::PAR_CODE_DESC matches 'YES|PAR|[PY].*' ? 'Y' : 'N') as PAR_CODE
, (data_in2::ALLOWED_DRG matches '^00[0-9][0-9][0-9]$' ? SUBSTRING(data_in2::ALLOWED_DRG,2,5) : 
		(data_in2::ALLOWED_DRG matches '^0[0-9][0-9][0-9]$' ? SUBSTRING(data_in2::ALLOWED_DRG,1,4) : 
			(data_in2::ALLOWED_DRG matches '^0[1-9][0-9][0-9][1-4]$' ? SUBSTRING(data_in2::ALLOWED_DRG,1,4) : 
				(data_in2::ALLOWED_DRG matches '^[1-9][0-9][0-9][1-4]$' ? SUBSTRING(data_in2::ALLOWED_DRG,0,3) : 
					(data_in2::ALLOWED_DRG matches '^[1-9]$' ? CONCAT('00',data_in2::ALLOWED_DRG) : 
						(data_in2::ALLOWED_DRG matches '^[0-9][1-9]$' ? CONCAT('0',data_in2::ALLOWED_DRG) : 
							(data_in2::ALLOWED_DRG matches '^[0-9][0-9][0-9]$' ? data_in2::ALLOWED_DRG : 
								(data_in2::ALLOWED_DRG matches '^000[0-9][0-9][0-9]$' ? SUBSTRING(data_in2::ALLOWED_DRG,3,6) : 
									(data_in2::ALLOWED_DRG matches '^00[1-9][0-9][0-9][1-4]$' ? SUBSTRING(data_in2::ALLOWED_DRG,2,5) : '000'))))))))) as DRG

, (JFIX5::ConceptID matches '.*MS.*' ? '_MS-DRG' : 
		(JFIX5::ConceptID matches '.*APR.*' ? '_APR-DRG' :
			(JFIX5::ConceptID matches '.*AP.*' ? '_AP-DRG' :	'$dflt_drg_type' ))) as DRG_TYPE

, (SUBSTRING(data_in2::ADMIT_DATE,0,4) < '2013' ? '16' : 
		(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2013' ? '25' :
			(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2014' ? '37' :
				(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2015' ? '45' :
					(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2016' ? '52' : '52'))))) as MS_GROUPER_KEY

,	(SUBSTRING(data_in2::ADMIT_DATE,0,4) < '2013' ? '23' : 
		(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2013' ? '28' :
			(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2014' ? '35' :
				(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2015' ? '49' :
						(SUBSTRING(data_in2::ADMIT_DATE,0,4) == '2016' ? '59' : '59'))))) as APR_GROUPER_KEY

,(data_in2::ADMIT_DIAG is null ? '0' : (data_in2::ADMIT_DIAG matches '0+' ? '0' : (data_in2::ADMIT_DIAG matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ADMIT_DIAG : '0'))) as ADMIT_DIAG
,(data_in2::CLAIM_DIAG_1 is null ? '0' : (data_in2::CLAIM_DIAG_1 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_1 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_1 : '0'))) as CLAIM_DIAG_1
,(data_in2::CLAIM_DIAG_2 is null ? '0' : (data_in2::CLAIM_DIAG_2 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_2 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_2 : '0'))) as CLAIM_DIAG_2
,(data_in2::CLAIM_DIAG_3 is null ? '0' : (data_in2::CLAIM_DIAG_3 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_3 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_3 : '0'))) as CLAIM_DIAG_3
,(data_in2::CLAIM_DIAG_4 is null ? '0' : (data_in2::CLAIM_DIAG_4 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_4 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_4 : '0'))) as CLAIM_DIAG_4
,(data_in2::CLAIM_DIAG_5 is null ? '0' : (data_in2::CLAIM_DIAG_5 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_5 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_5 : '0'))) as CLAIM_DIAG_5
,(data_in2::CLAIM_DIAG_6 is null ? '0' : (data_in2::CLAIM_DIAG_6 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_6 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_6 : '0'))) as CLAIM_DIAG_6
,(data_in2::CLAIM_DIAG_7 is null ? '0' : (data_in2::CLAIM_DIAG_7 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_7 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_7 : '0'))) as CLAIM_DIAG_7
,(data_in2::CLAIM_DIAG_8 is null ? '0' : (data_in2::CLAIM_DIAG_8 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_8 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_8 : '0'))) as CLAIM_DIAG_8
,(data_in2::CLAIM_DIAG_9 is null ? '0' : (data_in2::CLAIM_DIAG_9 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_9 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_9 : '0'))) as CLAIM_DIAG_9
,(data_in2::CLAIM_DIAG_10 is null ? '0' : (data_in2::CLAIM_DIAG_10 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_10 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_10 : '0'))) as CLAIM_DIAG_10
,(data_in2::CLAIM_DIAG_11 is null ? '0' : (data_in2::CLAIM_DIAG_11 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_11 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_11 : '0'))) as CLAIM_DIAG_11
,(data_in2::CLAIM_DIAG_12 is null ? '0' : (data_in2::CLAIM_DIAG_12 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_12 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_12 : '0'))) as CLAIM_DIAG_12
,(data_in2::CLAIM_DIAG_13 is null ? '0' : (data_in2::CLAIM_DIAG_13 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_13 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_13 : '0'))) as CLAIM_DIAG_13
,(data_in2::CLAIM_DIAG_14 is null ? '0' : (data_in2::CLAIM_DIAG_14 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_14 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_14 : '0'))) as CLAIM_DIAG_14
,(data_in2::CLAIM_DIAG_15 is null ? '0' : (data_in2::CLAIM_DIAG_15 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_15 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_15 : '0'))) as CLAIM_DIAG_15
,(data_in2::CLAIM_DIAG_16 is null ? '0' : (data_in2::CLAIM_DIAG_16 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_16 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_16 : '0'))) as CLAIM_DIAG_16
,(data_in2::CLAIM_DIAG_17 is null ? '0' : (data_in2::CLAIM_DIAG_17 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_17 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_17 : '0'))) as CLAIM_DIAG_17
,(data_in2::CLAIM_DIAG_18 is null ? '0' : (data_in2::CLAIM_DIAG_18 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_18 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_18 : '0'))) as CLAIM_DIAG_18
,(data_in2::CLAIM_DIAG_19 is null ? '0' : (data_in2::CLAIM_DIAG_19 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_19 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_19 : '0'))) as CLAIM_DIAG_19
,(data_in2::CLAIM_DIAG_20 is null ? '0' : (data_in2::CLAIM_DIAG_20 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_20 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_20 : '0'))) as CLAIM_DIAG_20
,(data_in2::CLAIM_DIAG_21 is null ? '0' : (data_in2::CLAIM_DIAG_21 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_21 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_21 : '0'))) as CLAIM_DIAG_21
,(data_in2::CLAIM_DIAG_22 is null ? '0' : (data_in2::CLAIM_DIAG_22 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_22 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_22 : '0'))) as CLAIM_DIAG_22
,(data_in2::CLAIM_DIAG_23 is null ? '0' : (data_in2::CLAIM_DIAG_23 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_23 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_23 : '0'))) as CLAIM_DIAG_23
,(data_in2::CLAIM_DIAG_24 is null ? '0' : (data_in2::CLAIM_DIAG_24 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_24 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_24 : '0'))) as CLAIM_DIAG_24
,(data_in2::CLAIM_DIAG_25 is null ? '0' : (data_in2::CLAIM_DIAG_25 matches '0+' ? '0' : (data_in2::CLAIM_DIAG_25 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::CLAIM_DIAG_25 : '0'))) as CLAIM_DIAG_25
,(data_in2::ICD_PROC_1 is null ? '0' : (data_in2::ICD_PROC_1 matches '0+' ? '0' : (data_in2::ICD_PROC_1 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_1 : '0'))) as ICD_PROC_1
,(data_in2::ICD_PROC_2 is null ? '0' : (data_in2::ICD_PROC_2 matches '0+' ? '0' : (data_in2::ICD_PROC_2 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_2 : '0'))) as ICD_PROC_2
,(data_in2::ICD_PROC_3 is null ? '0' : (data_in2::ICD_PROC_3 matches '0+' ? '0' : (data_in2::ICD_PROC_3 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_3 : '0'))) as ICD_PROC_3
,(data_in2::ICD_PROC_4 is null ? '0' : (data_in2::ICD_PROC_4 matches '0+' ? '0' : (data_in2::ICD_PROC_4 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_4 : '0'))) as ICD_PROC_4
,(data_in2::ICD_PROC_5 is null ? '0' : (data_in2::ICD_PROC_5 matches '0+' ? '0' : (data_in2::ICD_PROC_5 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_5 : '0'))) as ICD_PROC_5
,(data_in2::ICD_PROC_6 is null ? '0' : (data_in2::ICD_PROC_6 matches '0+' ? '0' : (data_in2::ICD_PROC_6 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_6 : '0'))) as ICD_PROC_6
,(data_in2::ICD_PROC_7 is null ? '0' : (data_in2::ICD_PROC_7 matches '0+' ? '0' : (data_in2::ICD_PROC_7 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_7 : '0'))) as ICD_PROC_7
,(data_in2::ICD_PROC_8 is null ? '0' : (data_in2::ICD_PROC_8 matches '0+' ? '0' : (data_in2::ICD_PROC_8 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_8 : '0'))) as ICD_PROC_8
,(data_in2::ICD_PROC_9 is null ? '0' : (data_in2::ICD_PROC_9 matches '0+' ? '0' : (data_in2::ICD_PROC_9 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_9 : '0'))) as ICD_PROC_9
,(data_in2::ICD_PROC_10 is null ? '0' : (data_in2::ICD_PROC_10 matches '0+' ? '0' : (data_in2::ICD_PROC_10 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_10 : '0'))) as ICD_PROC_10
,(data_in2::ICD_PROC_11 is null ? '0' : (data_in2::ICD_PROC_11 matches '0+' ? '0' : (data_in2::ICD_PROC_11 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_11 : '0'))) as ICD_PROC_11
,(data_in2::ICD_PROC_12 is null ? '0' : (data_in2::ICD_PROC_12 matches '0+' ? '0' : (data_in2::ICD_PROC_12 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_12 : '0'))) as ICD_PROC_12
,(data_in2::ICD_PROC_13 is null ? '0' : (data_in2::ICD_PROC_13 matches '0+' ? '0' : (data_in2::ICD_PROC_13 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_13 : '0'))) as ICD_PROC_13
,(data_in2::ICD_PROC_14 is null ? '0' : (data_in2::ICD_PROC_14 matches '0+' ? '0' : (data_in2::ICD_PROC_14 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_14 : '0'))) as ICD_PROC_14
,(data_in2::ICD_PROC_15 is null ? '0' : (data_in2::ICD_PROC_15 matches '0+' ? '0' : (data_in2::ICD_PROC_15 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_15 : '0'))) as ICD_PROC_15
,(data_in2::ICD_PROC_16 is null ? '0' : (data_in2::ICD_PROC_16 matches '0+' ? '0' : (data_in2::ICD_PROC_16 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_16 : '0'))) as ICD_PROC_16
,(data_in2::ICD_PROC_17 is null ? '0' : (data_in2::ICD_PROC_17 matches '0+' ? '0' : (data_in2::ICD_PROC_17 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_17 : '0'))) as ICD_PROC_17
,(data_in2::ICD_PROC_18 is null ? '0' : (data_in2::ICD_PROC_18 matches '0+' ? '0' : (data_in2::ICD_PROC_18 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_18 : '0'))) as ICD_PROC_18
,(data_in2::ICD_PROC_19 is null ? '0' : (data_in2::ICD_PROC_19 matches '0+' ? '0' : (data_in2::ICD_PROC_19 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_19 : '0'))) as ICD_PROC_19
,(data_in2::ICD_PROC_20 is null ? '0' : (data_in2::ICD_PROC_20 matches '0+' ? '0' : (data_in2::ICD_PROC_20 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_20 : '0'))) as ICD_PROC_20
,(data_in2::ICD_PROC_21 is null ? '0' : (data_in2::ICD_PROC_21 matches '0+' ? '0' : (data_in2::ICD_PROC_21 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_21 : '0'))) as ICD_PROC_21
,(data_in2::ICD_PROC_22 is null ? '0' : (data_in2::ICD_PROC_22 matches '0+' ? '0' : (data_in2::ICD_PROC_22 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_22 : '0'))) as ICD_PROC_22
,(data_in2::ICD_PROC_23 is null ? '0' : (data_in2::ICD_PROC_23 matches '0+' ? '0' : (data_in2::ICD_PROC_23 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_23 : '0'))) as ICD_PROC_23
,(data_in2::ICD_PROC_24 is null ? '0' : (data_in2::ICD_PROC_24 matches '0+' ? '0' : (data_in2::ICD_PROC_24 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_24 : '0'))) as ICD_PROC_24
,(data_in2::ICD_PROC_25 is null ? '0' : (data_in2::ICD_PROC_25 matches '0+' ? '0' : (data_in2::ICD_PROC_25 matches '[0-9A-Z][0-9A-Z][0-9A-Z]+' ? data_in2::ICD_PROC_25 : '0'))) as ICD_PROC_25

, (data_in2::Claim_UNITS is null ? 0 : data_in2::Claim_UNITS) as UNITS
, (data_in2::Claim_SUB_AMOUNT is null ? 0 : data_in2::Claim_SUB_AMOUNT) as CHARGES
, (data_in2::Claim_AMOUNT is null ? 0 : data_in2::Claim_AMOUNT) as ALLOWED
, (data_in2::Claim_COPAY is null ? 0 : data_in2::Claim_COPAY) as COPAY
, (data_in2::Claim_COINSURANCE is null ? 0 : data_in2::Claim_COINSURANCE) as COINS
, (data_in2::Claim_DEDUCTIBLE is null ? 0 : data_in2::Claim_DEDUCTIBLE) as DEDUCT
, (data_in2::Claim_COB is null ? 0 : data_in2::Claim_COB) as COB
, (data_in2::Claim_OTHER_REDUCTION is null ? 0 : data_in2::Claim_OTHER_REDUCTION) as OTHREDUCT
, (data_in2::Claim_PAID is null ? 0 : data_in2::Claim_PAID) as PAID
;



AB = FILTER AB1 BY ADMIT_DATE > '20121231' and DISCHARGE_DATE is not null and DISCHARGE_DATE > ADMIT_DATE and ALLOWED > 1000;

REQ_COLS_D0 = FOREACH AB GENERATE CLAIM_ID,ADMIT_DIAG,CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13, CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25,ICD_PROC_1, ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, ICD_PROC_7, ICD_PROC_8, ICD_PROC_9, ICD_PROC_10, ICD_PROC_11,ICD_PROC_12,ICD_PROC_13,ICD_PROC_14,ICD_PROC_15,ICD_PROC_16,ICD_PROC_17,ICD_PROC_18,ICD_PROC_19,ICD_PROC_20,ICD_PROC_21,ICD_PROC_22,ICD_PROC_23,ICD_PROC_24,ICD_PROC_25,version_check(ADMIT_DIAG,CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13, CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25,ICD_PROC_1, ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, ICD_PROC_7, ICD_PROC_8, ICD_PROC_9, ICD_PROC_10, ICD_PROC_11,ICD_PROC_12,ICD_PROC_13,ICD_PROC_14,ICD_PROC_15,ICD_PROC_16,ICD_PROC_17,ICD_PROC_18,ICD_PROC_19,ICD_PROC_20,ICD_PROC_21,ICD_PROC_22,ICD_PROC_23,ICD_PROC_24,ICD_PROC_25) AS VER;

REQ_COLS_D = DISTINCT REQ_COLS_D0;

REQ_COLS_D10 = FILTER REQ_COLS_D BY VER EQ 10;

REQ_DUMMY = FOREACH REQ_COLS_D10 GENERATE CLAIM_ID,ADMIT_DIAG,FLATTEN(diag_parser1(CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13, CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25)) AS (CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13, CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25,ECODE1,ECODE2,ECODE3,ECODE4,ECODE5,ECODE6,ECODE7,ECODE8,ECODE9,ECODE10),ICD_PROC_1, ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, ICD_PROC_7, ICD_PROC_8, ICD_PROC_9, ICD_PROC_10, ICD_PROC_11,ICD_PROC_12,ICD_PROC_13,ICD_PROC_14,ICD_PROC_15,ICD_PROC_16,ICD_PROC_17,ICD_PROC_18,ICD_PROC_19,ICD_PROC_20,ICD_PROC_21,ICD_PROC_22,ICD_PROC_23,ICD_PROC_24,ICD_PROC_25;

REQ_COLS_D9 = FILTER REQ_COLS_D BY VER EQ 9;

REQ_DUMMY9 = FOREACH REQ_COLS_D9 GENERATE CLAIM_ID,ADMIT_DIAG,FLATTEN(diag_parser2(CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13, CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25)) AS (CLAIM_DIAG_1, CLAIM_DIAG_2, CLAIM_DIAG_3, CLAIM_DIAG_4, CLAIM_DIAG_5, CLAIM_DIAG_6, CLAIM_DIAG_7, CLAIM_DIAG_8, CLAIM_DIAG_9, CLAIM_DIAG_10, CLAIM_DIAG_11, CLAIM_DIAG_12, CLAIM_DIAG_13, CLAIM_DIAG_14, CLAIM_DIAG_15, CLAIM_DIAG_16, CLAIM_DIAG_17, CLAIM_DIAG_18, CLAIM_DIAG_19, CLAIM_DIAG_20, CLAIM_DIAG_21, CLAIM_DIAG_22, CLAIM_DIAG_23, CLAIM_DIAG_24, CLAIM_DIAG_25,ECODE1,ECODE2,ECODE3,ECODE4,ECODE5,ECODE6,ECODE7,ECODE8,ECODE9,ECODE10),ICD_PROC_1, ICD_PROC_2, ICD_PROC_3, ICD_PROC_4, ICD_PROC_5, ICD_PROC_6, ICD_PROC_7, ICD_PROC_8, ICD_PROC_9, ICD_PROC_10, ICD_PROC_11,ICD_PROC_12,ICD_PROC_13,ICD_PROC_14,ICD_PROC_15,ICD_PROC_16,ICD_PROC_17,ICD_PROC_18,ICD_PROC_19,ICD_PROC_20,ICD_PROC_21,ICD_PROC_22,ICD_PROC_23,ICD_PROC_24,ICD_PROC_25;

diag1 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_1,1 as seq;
diag2 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_2,2 as seq;
diag3 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_3,3 as seq;
diag4 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_4,4 as seq;
diag5 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_5,5 as seq;
diag6 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_6,6 as seq;
diag7 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_7,7 as seq;
diag8 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_8,8 as seq;
diag9 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_9,9 as seq;
diag10 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_10,10 as seq;
diag11 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_11,11 as seq;
diag12 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_12,12 as seq;
diag13 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_13,13 as seq;
diag14 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_14,14 as seq;
diag15 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_15,15 as seq;
diag16 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_16,16 as seq;
diag17 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_17,17 as seq;
diag18 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_18,18 as seq;
diag19 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_19,19 as seq;
diag20 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_20,20 as seq;
diag21 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_21,21 as seq;
diag22 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_22,22 as seq;
diag23 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_23,23 as seq;
diag24 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_24,24 as seq;
diag25 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,CLAIM_DIAG_25,25 as seq;
admit_diag = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ADMIT_DIAG,26 as seq;
ecode1 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE1,27 as seq;
ecode2 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE2,28 as seq;
ecode3 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE3,29 as seq;
ecode4 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE4,30 as seq;
ecode5 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE5,31 as seq;
ecode6 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE6,32 as seq;
ecode7 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE7,33 as seq;
ecode8 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE8,34 as seq;
ecode9 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE9,35 as seq;
ecode10 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ECODE10,36 as seq;

diags0 = UNION diag1,diag2,diag3,diag4,diag5,diag6,diag7,diag8,diag9,diag10,diag11,diag12,diag13,diag14,diag15,diag16,diag17,diag18,diag19,diag20,diag21,diag22,diag23,diag24,diag25,admit_diag,ecode1,ecode2,ecode3,ecode4,ecode5,ecode6,ecode7,ecode8,ecode9,ecode10;
diags00 = FILTER diags0 BY $1 is not null;

diags = FOREACH diags00 GENERATE $0 as CLAIM_ID, $1 as diag, $2 as seq;

join_diags_icddiag10to9 = JOIN diags BY diag LEFT,icddiag10to9 BY $0 using 'Replicated';
converted_diags = FOREACH join_diags_icddiag10to9 GENERATE $0 as CLAIM_ID, ($4 is null ? $1 : $4) as diag,$2 as seq;

con_diag_row0= FOREACH (group converted_diags BY CLAIM_ID)
			{
				temp = FOREACH converted_diags GENERATE diag,seq;
				temp1 = FILTER temp BY seq <= 25;
				temp2 = ORDER temp1 BY seq ASC;
				temp3 = FILTER temp BY seq eq 26;
				temp4 = FILTER temp BY seq>=27 and seq<=36;
				temp5 = ORDER temp4 BY seq ASC;
				GENERATE FLATTEN(group) as CLAIM_ID,  BagToString(temp2.diag,'|') as claim_diag, BagToString(temp3.diag,'|') AS admit_diag, BagToString(temp5.diag,'|') AS ecode_diag;
			}

con_diag_row1 = DISTINCT con_diag_row0;
limit_with_req_diag = FOREACH con_diag_row1 GENERATE CLAIM_ID,FLATTEN(pic_25_diags(claim_diag)) as (diag1,diag2,diag3,diag4,diag5,diag6,diag7,diag8,diag9,diag10,diag11,diag12,diag13,diag14,diag15,diag16,diag17,diag18,diag19,diag20,diag21,diag22,diag23,diag24,diag25),FLATTEN(pic_1_col(admit_diag)) AS admit_diag,FLATTEN(pic_10_cols(ecode_diag)) as (ediag1,ediag2,ediag3,ediag4,ediag5,ediag6,ediag7,ediag8,ediag9,ediag10);
--

proc1 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_1,1 as seq;
proc2 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_2,2 as seq;
proc3 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_3,3 as seq;
proc4 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_4,4 as seq;
proc5 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_5,5 as seq;
proc6 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_6,6 as seq;
proc7 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_7,7 as seq;
proc8 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_8,8 as seq;
proc9 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_9,9 as seq;
proc10 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_10,10 as seq;
proc11 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_11,11 as seq;
proc12 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_12,12 as seq;
proc13 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_13,13 as seq;
proc14 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_14,14 as seq;
proc15 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_15,15 as seq;
proc16 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_16,16 as seq;
proc17 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_17,17 as seq;
proc18 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_18,18 as seq;
proc19 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_19,19 as seq;
proc20 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_20,20 as seq;
proc21 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_21,21 as seq;
proc22 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_22,22 as seq;
proc23 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_23,23 as seq;
proc24 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_24,24 as seq;
proc25 = FOREACH REQ_DUMMY GENERATE CLAIM_ID,ICD_PROC_25,25 as seq;

procs0 = UNION proc1,proc2,proc3,proc4,proc5,proc6,proc7,proc8,proc9,proc10,proc11,proc12,proc13,proc14,proc15,proc16,proc17,proc18,proc19,proc20,proc21,proc22,proc23,proc24,proc25;
procs00 = FILTER procs0 BY $1 is not null;
procs = FOREACH procs00 GENERATE $0 as CLAIM_ID, remove_period_proc($1) as proc, $2 as seq;

join_procs_icdproc10to9 = JOIN procs BY proc LEFT,icdproc10to9 BY $0 using 'Replicated';
converted_procs = FOREACH join_procs_icdproc10to9 GENERATE $0 as CLAIM_ID, ($4 is null ? $1 : $4) as proc,$2 as seq;

con_proc_row0= FOREACH (group converted_procs BY CLAIM_ID)
			{
				temp = FOREACH converted_procs GENERATE proc,seq;
				temp2 = ORDER temp BY seq ASC;
				GENERATE FLATTEN(group) as CLAIM_ID, BagToString(temp2.proc,'|') as claim_proc;
			}
con_proc_row1 = DISTINCT con_proc_row0;

limit_with_req_proc = FOREACH con_proc_row1 GENERATE CLAIM_ID, FLATTEN(pic_25_cols(claim_proc)) as (proc1,proc2,proc3,proc4,proc5,proc6,proc7,proc8,proc9,proc10,proc11,proc12,proc13,proc14,proc15,proc16,proc17,proc18,proc19,proc20,proc21,proc22,proc23,proc24,proc25);

join_limit_with_req_proc_diag0 = JOIN AB BY CLAIM_ID, limit_with_req_diag BY CLAIM_ID, limit_with_req_proc BY CLAIM_ID;


converted_10 = FOREACH join_limit_with_req_proc_diag0 GENERATE AB::PAYER as PAYER,AB::CLAIM_ID as CLAIM_ID,AB::ConceptID as ConceptID,AB::LINE_SEQ as LINE_SEQ,AB::ADMIT_KEY as ADMIT_KEY,AB::PROVIDER as PROVIDER,AB::ADMIT_DATE as ADMIT_DATE,AB::DISCHARGE_DATE as DISCHARGE_DATE,AB::DISCHARGE_STATUS as DISCHARGE_STATUS,AB::MEMBER as MEMBER,AB::GENDER as GENDER,AB::LOB as LOB,AB::DOB as DOB,AB::BILL_TYPE as BILL_TYPE,AB::PAR_CODE as PAR_CODE,AB::DRG as DRG,AB::DRG_TYPE as DRG_TYPE,AB::MS_GROUPER_KEY as MS_GROUPER_KEY,AB::APR_GROUPER_KEY as APR_GROUPER_KEY,AB::UNITS as UNITS,AB::CHARGES as CHARGES,AB::ALLOWED as ALLOWED,AB::COPAY as COPAY,AB::COINS as COINS,AB::DEDUCT as DEDUCT,AB::COB as COB,AB::OTHREDUCT as OTHREDUCT,AB::PAID as PAID,limit_with_req_diag::diag1 as diag1,limit_with_req_diag::diag2 as diag2,limit_with_req_diag::diag3 as diag3,limit_with_req_diag::diag4 as diag4,limit_with_req_diag::diag5 as diag5,limit_with_req_diag::diag6 as diag6,limit_with_req_diag::diag7 as diag7,limit_with_req_diag::diag8 as diag8,limit_with_req_diag::diag9 as diag9,limit_with_req_diag::diag10 as diag10,limit_with_req_diag::diag11 as diag11,limit_with_req_diag::diag12 as diag12,limit_with_req_diag::diag13 as diag13,limit_with_req_diag::diag14 as diag14,limit_with_req_diag::diag15 as diag15,limit_with_req_diag::diag16 as diag16,limit_with_req_diag::diag17 as diag17,limit_with_req_diag::diag18 as diag18,limit_with_req_diag::diag19 as diag19,limit_with_req_diag::diag20 as diag20,limit_with_req_diag::diag21 as diag21,limit_with_req_diag::diag22 as diag22,limit_with_req_diag::diag23 as diag23,limit_with_req_diag::diag24 as diag24,limit_with_req_diag::diag25 as diag25,limit_with_req_diag::admit_diag as admit_diag,limit_with_req_diag::ediag1 as ediag1,limit_with_req_diag::ediag2 as ediag2,limit_with_req_diag::ediag3 as ediag3,limit_with_req_diag::ediag4 as ediag4,limit_with_req_diag::ediag5 as ediag5,limit_with_req_diag::ediag6 as ediag6,limit_with_req_diag::ediag7 as ediag7,limit_with_req_diag::ediag8 as ediag8,limit_with_req_diag::ediag9 as ediag9,limit_with_req_diag::ediag10 as ediag10,limit_with_req_proc::proc1 as icd_proc_1,limit_with_req_proc::proc2 as icd_proc_2,limit_with_req_proc::proc3 as icd_proc_3,limit_with_req_proc::proc4 as icd_proc_4,limit_with_req_proc::proc5 as icd_proc_5,limit_with_req_proc::proc6 as icd_proc_6,limit_with_req_proc::proc7 as icd_proc_7,limit_with_req_proc::proc8 as icd_proc_8,limit_with_req_proc::proc9 as icd_proc_9,limit_with_req_proc::proc10 as icd_proc_10,limit_with_req_proc::proc11 as icd_proc_11,limit_with_req_proc::proc12 as icd_proc_12,limit_with_req_proc::proc13 as icd_proc_13,limit_with_req_proc::proc14 as icd_proc_14,limit_with_req_proc::proc15 as icd_proc_15,limit_with_req_proc::proc16 as icd_proc_16,limit_with_req_proc::proc17 as icd_proc_17,limit_with_req_proc::proc18 as icd_proc_18,limit_with_req_proc::proc19 as icd_proc_19,limit_with_req_proc::proc20 as icd_proc_20,limit_with_req_proc::proc21 as icd_proc_21,limit_with_req_proc::proc22 as icd_proc_22,limit_with_req_proc::proc23 as icd_proc_23,limit_with_req_proc::proc24 as icd_proc_24,limit_with_req_proc::proc25 as icd_proc_25;

join_limit_with_req_proc_diag1 = JOIN AB BY CLAIM_ID, REQ_DUMMY9 BY CLAIM_ID;

converted_9 = FOREACH join_limit_with_req_proc_diag1 GENERATE AB::PAYER as PAYER,AB::CLAIM_ID as CLAIM_ID,AB::ConceptID as ConceptID,AB::LINE_SEQ as LINE_SEQ,AB::ADMIT_KEY as ADMIT_KEY,AB::PROVIDER as PROVIDER,AB::ADMIT_DATE as ADMIT_DATE,AB::DISCHARGE_DATE as DISCHARGE_DATE,AB::DISCHARGE_STATUS as DISCHARGE_STATUS,AB::MEMBER as MEMBER,AB::GENDER as GENDER,AB::LOB as LOB,AB::DOB as DOB,AB::BILL_TYPE as BILL_TYPE,AB::PAR_CODE as PAR_CODE,AB::DRG as DRG,AB::DRG_TYPE as DRG_TYPE,AB::MS_GROUPER_KEY as MS_GROUPER_KEY,AB::APR_GROUPER_KEY as APR_GROUPER_KEY,AB::UNITS as UNITS,AB::CHARGES as CHARGES,AB::ALLOWED as ALLOWED,AB::COPAY as COPAY,AB::COINS as COINS,AB::DEDUCT as DEDUCT,AB::COB as COB,AB::OTHREDUCT as OTHREDUCT,AB::PAID as PAID,REQ_DUMMY9::CLAIM_DIAG_1 as claim_diag_1,REQ_DUMMY9::CLAIM_DIAG_2 as claim_diag_2,REQ_DUMMY9::CLAIM_DIAG_3 as claim_diag_3,REQ_DUMMY9::CLAIM_DIAG_4 as claim_diag_4,REQ_DUMMY9::CLAIM_DIAG_5 as claim_diag_5,REQ_DUMMY9::CLAIM_DIAG_6 as claim_diag_6,REQ_DUMMY9::CLAIM_DIAG_7 as claim_diag_7,REQ_DUMMY9::CLAIM_DIAG_8 as claim_diag_8,REQ_DUMMY9::CLAIM_DIAG_9 as claim_diag_9,REQ_DUMMY9::CLAIM_DIAG_10 as claim_diag_10,REQ_DUMMY9::CLAIM_DIAG_11 as claim_diag_11,REQ_DUMMY9::CLAIM_DIAG_12 as claim_diag_12,REQ_DUMMY9::CLAIM_DIAG_13 as claim_diag_13,REQ_DUMMY9::CLAIM_DIAG_14 as claim_diag_14,REQ_DUMMY9::CLAIM_DIAG_15 as claim_diag_15,REQ_DUMMY9::CLAIM_DIAG_16 as claim_diag_16,REQ_DUMMY9::CLAIM_DIAG_17 as claim_diag_17,REQ_DUMMY9::CLAIM_DIAG_18 as claim_diag_18,REQ_DUMMY9::CLAIM_DIAG_19 as claim_diag_19,REQ_DUMMY9::CLAIM_DIAG_20 as claim_diag_20,REQ_DUMMY9::CLAIM_DIAG_21 as claim_diag_21,REQ_DUMMY9::CLAIM_DIAG_22 as claim_diag_22,REQ_DUMMY9::CLAIM_DIAG_23 as claim_diag_23,REQ_DUMMY9::CLAIM_DIAG_24 as claim_diag_24,REQ_DUMMY9::CLAIM_DIAG_25 as claim_diag_25,REQ_DUMMY9::ADMIT_DIAG as admit_diag,REQ_DUMMY9::ECODE1 as e_diag_1,REQ_DUMMY9::ECODE2 as e_diag_2,REQ_DUMMY9::ECODE3 as e_diag_3,REQ_DUMMY9::ECODE4 as e_diag_4,REQ_DUMMY9::ECODE5 as e_diag_5,REQ_DUMMY9::ECODE6 as e_diag_6,REQ_DUMMY9::ECODE7 as e_diag_7,REQ_DUMMY9::ECODE8 as e_diag_8,REQ_DUMMY9::ECODE9 as e_diag_9,REQ_DUMMY9::ECODE10 as e_diag_10,REQ_DUMMY9::ICD_PROC_1 as icd_proc_1,REQ_DUMMY9::ICD_PROC_2 as icd_proc_2,REQ_DUMMY9::ICD_PROC_3 as icd_proc_3,REQ_DUMMY9::ICD_PROC_4 as icd_proc_4,REQ_DUMMY9::ICD_PROC_5 as icd_proc_5,REQ_DUMMY9::ICD_PROC_6 as icd_proc_6,REQ_DUMMY9::ICD_PROC_7 as icd_proc_7,REQ_DUMMY9::ICD_PROC_8 as icd_proc_8,REQ_DUMMY9::ICD_PROC_9 as icd_proc_9,REQ_DUMMY9::ICD_PROC_10 as icd_proc_10,REQ_DUMMY9::ICD_PROC_11 as icd_proc_11,REQ_DUMMY9::ICD_PROC_12 as icd_proc_12,REQ_DUMMY9::ICD_PROC_13 as icd_proc_13,REQ_DUMMY9::ICD_PROC_14 as icd_proc_14,REQ_DUMMY9::ICD_PROC_15 as icd_proc_15,REQ_DUMMY9::ICD_PROC_16 as icd_proc_16,REQ_DUMMY9::ICD_PROC_17 as icd_proc_17,REQ_DUMMY9::ICD_PROC_18 as icd_proc_18,REQ_DUMMY9::ICD_PROC_19 as icd_proc_19,REQ_DUMMY9::ICD_PROC_20 as icd_proc_20,REQ_DUMMY9::ICD_PROC_21 as icd_proc_21,REQ_DUMMY9::ICD_PROC_22 as icd_proc_22,REQ_DUMMY9::ICD_PROC_23 as icd_proc_23,REQ_DUMMY9::ICD_PROC_24 as icd_proc_24,REQ_DUMMY9::ICD_PROC_25 as icd_proc_25;

AI = UNION converted_9,converted_10;

AJ = Group AI by (PAYER, ADMIT_KEY, ADMIT_DATE, MEMBER, PROVIDER, ConceptID, DRG_TYPE, MS_GROUPER_KEY, APR_GROUPER_KEY);

AK = Foreach AJ Generate FLATTEN(group) as (PAYER, ADMIT_KEY, ADMIT_DATE, MEMBER, PROVIDER, ConceptID, DRG_TYPE, MS_GROUPER_KEY, APR_GROUPER_KEY)
, COUNT(AI.LINE_SEQ) as LINES
, MAX(AI.DISCHARGE_DATE) as DISCHARGE_DATE
, MIN(AI.DISCHARGE_STATUS) as MIN_DISCHARGE_STATUS
, MAX(AI.DISCHARGE_STATUS) as MAX_DISCHARGE_STATUS
, MAX(AI.GENDER) as GENDER
, MAX(AI.LOB) AS LOB
, MAX(AI.DOB) AS DOB
, MAX(AI.BILL_TYPE) as BILL_TYPE
, MAX(AI.PAR_CODE) as PAR_CODE
, MAX(AI.DRG) as DRG
, MAX(AI.claim_diag_1)  as claim_diag_1
, MAX(AI.claim_diag_2) as claim_diag_2
, MAX(AI.claim_diag_3) as claim_diag_3
, MAX(AI.claim_diag_4) as claim_diag_4
, MAX(AI.claim_diag_5) as claim_diag_5
, MAX(AI.claim_diag_6) as claim_diag_6
, MAX(AI.claim_diag_7) as claim_diag_7
, MAX(AI.claim_diag_8) as claim_diag_8
, MAX(AI.claim_diag_9) as claim_diag_9
, MAX(AI.claim_diag_10) as claim_diag_10
, MAX(AI.claim_diag_11) as claim_diag_11
, MAX(AI.claim_diag_12) as claim_diag_12
, MAX(AI.claim_diag_13) as claim_diag_13
, MAX(AI.claim_diag_14) as claim_diag_14
, MAX(AI.claim_diag_15) as claim_diag_15
, MAX(AI.claim_diag_16) as claim_diag_16
, MAX(AI.claim_diag_17) as claim_diag_17
, MAX(AI.claim_diag_18) as claim_diag_18
, MAX(AI.claim_diag_19) as claim_diag_19
, MAX(AI.claim_diag_20) as claim_diag_20
, MAX(AI.claim_diag_21) as claim_diag_21
, MAX(AI.claim_diag_22) as claim_diag_22
, MAX(AI.claim_diag_23) as claim_diag_23
, MAX(AI.claim_diag_24) as claim_diag_24
, MAX(AI.claim_diag_25) as claim_diag_25
, MAX(AI.e_diag_1) as e_diag_1
, MAX(AI.e_diag_2) as e_diag_2
, MAX(AI.e_diag_3) as e_diag_3
, MAX(AI.admit_diag) as admit_diag
, MAX(AI.icd_proc_1) as icd_proc_1
, MAX(AI.icd_proc_2) as icd_proc_2
, MAX(AI.icd_proc_3) as icd_proc_3
, MAX(AI.icd_proc_4) as icd_proc_4
, MAX(AI.icd_proc_5) as icd_proc_5
, MAX(AI.icd_proc_6) as icd_proc_6
, MAX(AI.icd_proc_7) as icd_proc_7
, MAX(AI.icd_proc_8) as icd_proc_8
, MAX(AI.icd_proc_9) as icd_proc_9
, MAX(AI.icd_proc_10) as icd_proc_10
, MAX(AI.icd_proc_11) as icd_proc_11
, MAX(AI.icd_proc_12) as icd_proc_12
, MAX(AI.icd_proc_13) as icd_proc_13
, MAX(AI.icd_proc_14) as icd_proc_14
, MAX(AI.icd_proc_15) as icd_proc_15
, MAX(AI.icd_proc_16) as icd_proc_16
, MAX(AI.icd_proc_17) as icd_proc_17
, MAX(AI.icd_proc_18) as icd_proc_18
, MAX(AI.icd_proc_19) as icd_proc_19
, MAX(AI.icd_proc_20) as icd_proc_20
, MAX(AI.icd_proc_21) as icd_proc_21
, MAX(AI.icd_proc_22) as icd_proc_22
, MAX(AI.icd_proc_23) as icd_proc_23
, MAX(AI.icd_proc_24) as icd_proc_24
, MAX(AI.icd_proc_25) as icd_proc_25
, MAX(AI.UNITS) as UNITS
, MAX(AI.CHARGES) as CHARGES
, MAX(AI.ALLOWED) as ALLOWED
, MAX(AI.COPAY) as COPAY
, MAX(AI.COINS) as COINS
, MAX(AI.DEDUCT) as DEDUCT
, MAX(AI.COB) as COB
, MAX(AI.OTHREDUCT) as OTHREDUCT
, MAX(AI.PAID) as PAID;

AL = Foreach AK Generate PAYER, ADMIT_KEY, ADMIT_DATE, MEMBER, PROVIDER, ConceptID
,LINES
,DISCHARGE_DATE
,MIN_DISCHARGE_STATUS
,MAX_DISCHARGE_STATUS
,GENDER
,LOB
,DOB
,BILL_TYPE
,PAR_CODE
,DRG
,DRG_TYPE
,MS_GROUPER_KEY
,APR_GROUPER_KEY
,'43' as AP_GROUPER_KEY
,claim_diag_1
,claim_diag_2
,claim_diag_3
,claim_diag_4
,claim_diag_5
,claim_diag_6
,claim_diag_7
,claim_diag_8
,claim_diag_9
,claim_diag_10
,claim_diag_11
,claim_diag_12
,claim_diag_13
,claim_diag_14
,claim_diag_15
,claim_diag_16
,claim_diag_17
,claim_diag_18
,claim_diag_19
,claim_diag_20
,claim_diag_21
,claim_diag_22
,claim_diag_23
,claim_diag_24
,claim_diag_25
,e_diag_1
,e_diag_2
,e_diag_3
,admit_diag
,icd_proc_1
,icd_proc_2
,icd_proc_3
,icd_proc_4
,icd_proc_5
,icd_proc_6
,icd_proc_7
,icd_proc_8
,icd_proc_9
,icd_proc_10
,icd_proc_11
,icd_proc_12
,icd_proc_13
,icd_proc_14
,icd_proc_15
,icd_proc_16
,icd_proc_17
,icd_proc_18
,icd_proc_19
,icd_proc_20
,icd_proc_21
,icd_proc_22
,icd_proc_23
,icd_proc_24
,icd_proc_25
,UNITS
,CHARGES
,ALLOWED
,COPAY
,COINS
,DEDUCT
,COB
,OTHREDUCT
,PAID;

APRSOI1 = FILTER AL by DRG_TYPE == '_APR-DRG';
APRSOI2 = Foreach APRSOI1 Generate PAYER, ALLOWED;
APRSOI3 = Group APRSOI2 by PAYER;
APRSOI4 = Foreach APRSOI3 Generate FLATTEN(group) as PAYER, AVG(APRSOI2.ALLOWED) as AVG_ALLOWED, FLATTEN(Median(APRSOI2.ALLOWED)) as MED_ALLOWED;

AM2 = Foreach AL Generate PAYER, ADMIT_KEY, ADMIT_DATE, MEMBER, PROVIDER, ConceptID
,LINES
,DISCHARGE_DATE
,MIN_DISCHARGE_STATUS
,MAX_DISCHARGE_STATUS
,GENDER
,LOB
,DOB
,BILL_TYPE
,PAR_CODE
,DRG
,DRG_TYPE
,MS_GROUPER_KEY
,APR_GROUPER_KEY
,AP_GROUPER_KEY
,claim_diag_1
,claim_diag_2
,claim_diag_3
,claim_diag_4
,claim_diag_5
,claim_diag_6
,claim_diag_7
,claim_diag_8
,claim_diag_9
,claim_diag_10
,claim_diag_11
,claim_diag_12
,claim_diag_13
,claim_diag_14
,claim_diag_15
,claim_diag_16
,claim_diag_17
,claim_diag_18
,claim_diag_19
,claim_diag_20
,claim_diag_21
,claim_diag_22
,claim_diag_23
,claim_diag_24
,claim_diag_25
,e_diag_1
,e_diag_2
,e_diag_3
,admit_diag
,icd_proc_1
,icd_proc_2
,icd_proc_3
,icd_proc_4
,icd_proc_5
,icd_proc_6
,icd_proc_7
,icd_proc_8
,icd_proc_9
,icd_proc_10
,icd_proc_11
,icd_proc_12
,icd_proc_13
,icd_proc_14
,icd_proc_15
,icd_proc_16
,icd_proc_17
,icd_proc_18
,icd_proc_19
,icd_proc_20
,icd_proc_21
,icd_proc_22
,icd_proc_23
,icd_proc_24
,icd_proc_25
,UNITS
,CHARGES
,ALLOWED
,COPAY
,COINS
,DEDUCT
,COB
,OTHREDUCT
,PAID;

AM1 = Join AM2 by PAYER LEFT OUTER, APRSOI4 by PAYER using 'replicated';

AM = Foreach AM1 Generate AM2::PAYER as PAYER
,AM2::ADMIT_KEY as  ADMIT_KEY
,AM2::ADMIT_DATE as  ADMIT_DATE
,AM2::MEMBER as  MEMBER
,AM2::PROVIDER as  PROVIDER
,AM2::ConceptID as  ConceptID
,AM2::LINES as  LINES
,AM2::DISCHARGE_DATE as  DISCHARGE_DATE
,AM2::MIN_DISCHARGE_STATUS as  MIN_DISCHARGE_STATUS
,AM2::MAX_DISCHARGE_STATUS as  MAX_DISCHARGE_STATUS
,AM2::GENDER as  GENDER
,AM2::LOB as  LOB
,AM2::DOB as  DOB
,AM2::BILL_TYPE as  BILL_TYPE
,AM2::PAR_CODE as  PAR_CODE
,AM2::DRG as  DRG
,(APRSOI4::AVG_ALLOWED is null ? 1 : AM2::ALLOWED/APRSOI4::AVG_ALLOWED) as APPROX_APR_WEIGHT
,AM2::DRG_TYPE as  DRG_TYPE
,AM2::MS_GROUPER_KEY as MS_GROUPER_KEY
,AM2::APR_GROUPER_KEY as APR_GROUPER_KEY
,AM2::AP_GROUPER_KEY as AP_GROUPER_KEY
,AM2::claim_diag_1 as  claim_diag_1
,AM2::claim_diag_2 as  claim_diag_2
,AM2::claim_diag_3 as  claim_diag_3
,AM2::claim_diag_4 as  claim_diag_4
,AM2::claim_diag_5 as  claim_diag_5
,AM2::claim_diag_6 as  claim_diag_6
,AM2::claim_diag_7 as  claim_diag_7
,AM2::claim_diag_8 as  claim_diag_8
,AM2::claim_diag_9 as  claim_diag_9
,AM2::claim_diag_10 as  claim_diag_10
,AM2::claim_diag_11 as  claim_diag_11
,AM2::claim_diag_12 as  claim_diag_12
,AM2::claim_diag_13 as  claim_diag_13
,AM2::claim_diag_14 as  claim_diag_14
,AM2::claim_diag_15 as  claim_diag_15
,AM2::claim_diag_16 as  claim_diag_16
,AM2::claim_diag_17 as  claim_diag_17
,AM2::claim_diag_18 as  claim_diag_18
,AM2::claim_diag_19 as  claim_diag_19
,AM2::claim_diag_20 as  claim_diag_20
,AM2::claim_diag_21 as  claim_diag_21
,AM2::claim_diag_22 as  claim_diag_22
,AM2::claim_diag_23 as  claim_diag_23
,AM2::claim_diag_24 as  claim_diag_24
,AM2::claim_diag_25 as  claim_diag_25
,AM2::e_diag_1 as  e_diag_1
,AM2::e_diag_2 as  e_diag_2
,AM2::e_diag_3 as  e_diag_3
,AM2::admit_diag as  admit_diag
,AM2::icd_proc_1 as  icd_proc_1
,AM2::icd_proc_2 as  icd_proc_2
,AM2::icd_proc_3 as  icd_proc_3
,AM2::icd_proc_4 as  icd_proc_4
,AM2::icd_proc_5 as  icd_proc_5
,AM2::icd_proc_6 as  icd_proc_6
,AM2::icd_proc_7 as  icd_proc_7
,AM2::icd_proc_8 as  icd_proc_8
,AM2::icd_proc_9 as  icd_proc_9
,AM2::icd_proc_10 as  icd_proc_10
,AM2::icd_proc_11 as  icd_proc_11
,AM2::icd_proc_12 as  icd_proc_12
,AM2::icd_proc_13 as  icd_proc_13
,AM2::icd_proc_14 as  icd_proc_14
,AM2::icd_proc_15 as  icd_proc_15
,AM2::icd_proc_16 as  icd_proc_16
,AM2::icd_proc_17 as  icd_proc_17
,AM2::icd_proc_18 as  icd_proc_18
,AM2::icd_proc_19 as  icd_proc_19
,AM2::icd_proc_20 as  icd_proc_20
,AM2::icd_proc_21 as  icd_proc_21
,AM2::icd_proc_22 as  icd_proc_22
,AM2::icd_proc_23 as  icd_proc_23
,AM2::icd_proc_24 as  icd_proc_24
,AM2::icd_proc_25 as  icd_proc_25
,AM2::UNITS as  UNITS
,AM2::CHARGES as  CHARGES
,AM2::ALLOWED as  ALLOWED
,AM2::COPAY as  COPAY
,AM2::COINS as  COINS
,AM2::DEDUCT as  DEDUCT
,AM2::COB as  COB
,AM2::OTHREDUCT as  OTHREDUCT
,AM2::PAID as  PAID;


DRG_CODE_MASTER = LOAD '$DRG_MASTER_PATH' USING PigStorage('\\u1') AS $DRG_MASTER_SCHEMA;

DRGCM1 = Foreach DRG_CODE_MASTER Generate drg_code, (soi is null ? '0' : soi) as soi, grouper_key, drg_weight, gmlos;

DRGCM2 = Group DRGCM1 by (drg_code, grouper_key);

DRGCM3 = Foreach DRGCM2 {
				 DRGCM3_1 = FILTER DRGCM1 by soi == '0';
				 DRGCM3_2 = DRGCM3_1.drg_weight;
				 DRGCM3_3 = DRGCM3_1.gmlos;
				 DRGCM3_4 = FILTER DRGCM1 by soi == '1';
				 DRGCM3_5 = DRGCM3_4.drg_weight;
				 DRGCM3_6 = DRGCM3_4.gmlos;
				 DRGCM3_7 = FILTER DRGCM1 by soi == '2';
				 DRGCM3_8 = DRGCM3_7.drg_weight;
				 DRGCM3_9 = DRGCM3_7.gmlos;
				 DRGCM3_10 = FILTER DRGCM1 by soi == '3';
				 DRGCM3_11 = DRGCM3_10.drg_weight;
				 DRGCM3_12 = DRGCM3_10.gmlos;
				 DRGCM3_13 = FILTER DRGCM1 by soi == '4';
				 DRGCM3_14 = DRGCM3_13.drg_weight;
				 DRGCM3_15 = DRGCM3_13.gmlos;
				 Generate FLATTEN(group) as (drg_code, grouper_key), MAX(DRGCM3_2) as SOI0_drg_weight
					, MAX(DRGCM3_3) as SOI0_drg_gmlos
					, MAX(DRGCM3_5) as SOI1_drg_weight
					, MAX(DRGCM3_6) as SOI1_drg_gmlos
					, MAX(DRGCM3_8) as SOI2_drg_weight
					, MAX(DRGCM3_9) as SOI2_drg_gmlos
					, MAX(DRGCM3_11) as SOI3_drg_weight
					, MAX(DRGCM3_12) as SOI3_drg_gmlos
					, MAX(DRGCM3_14) as SOI4_drg_weight
					, MAX(DRGCM3_15) as SOI4_drg_gmlos;
};

AN = Join AM by (DRG, MS_GROUPER_KEY) LEFT OUTER, DRGCM3 by (drg_code, grouper_key) using 'replicated';

AN2 = Foreach AN Generate AM::PAYER as PAYER
,AM::ADMIT_KEY as  ADMIT_KEY
,AM::ADMIT_DATE as  ADMIT_DATE
,AM::MEMBER as  MEMBER
,AM::PROVIDER as  PROVIDER
,AM::ConceptID as  ConceptID
,AM::LINES as  LINES
,AM::DISCHARGE_DATE as  DISCHARGE_DATE
,AM::MIN_DISCHARGE_STATUS as  MIN_DISCHARGE_STATUS
,AM::MAX_DISCHARGE_STATUS as  MAX_DISCHARGE_STATUS
,AM::GENDER as  GENDER
,AM::LOB as  LOB
,AM::DOB as  DOB
,AM::BILL_TYPE as  BILL_TYPE
,AM::PAR_CODE as  PAR_CODE
,AM::DRG as  DRG
,AM::APPROX_APR_WEIGHT as  APPROX_APR_WEIGHT
,AM::DRG_TYPE as  DRG_TYPE
,AM::MS_GROUPER_KEY as MS_GROUPER_KEY
,DRGCM3::SOI0_drg_weight as MS_drg_weight
,DRGCM3::SOI0_drg_gmlos as MS_drg_gmlos
,AM::APR_GROUPER_KEY as APR_GROUPER_KEY
,AM::AP_GROUPER_KEY as AP_GROUPER_KEY
,AM::claim_diag_1 as  claim_diag_1
,AM::claim_diag_2 as  claim_diag_2
,AM::claim_diag_3 as  claim_diag_3
,AM::claim_diag_4 as  claim_diag_4
,AM::claim_diag_5 as  claim_diag_5
,AM::claim_diag_6 as  claim_diag_6
,AM::claim_diag_7 as  claim_diag_7
,AM::claim_diag_8 as  claim_diag_8
,AM::claim_diag_9 as  claim_diag_9
,AM::claim_diag_10 as  claim_diag_10
,AM::claim_diag_11 as  claim_diag_11
,AM::claim_diag_12 as  claim_diag_12
,AM::claim_diag_13 as  claim_diag_13
,AM::claim_diag_14 as  claim_diag_14
,AM::claim_diag_15 as  claim_diag_15
,AM::claim_diag_16 as  claim_diag_16
,AM::claim_diag_17 as  claim_diag_17
,AM::claim_diag_18 as  claim_diag_18
,AM::claim_diag_19 as  claim_diag_19
,AM::claim_diag_20 as  claim_diag_20
,AM::claim_diag_21 as  claim_diag_21
,AM::claim_diag_22 as  claim_diag_22
,AM::claim_diag_23 as  claim_diag_23
,AM::claim_diag_24 as  claim_diag_24
,AM::claim_diag_25 as  claim_diag_25
,AM::e_diag_1 as  e_diag_1
,AM::e_diag_2 as  e_diag_2
,AM::e_diag_3 as  e_diag_3
,AM::admit_diag as  admit_diag
,AM::icd_proc_1 as  icd_proc_1
,AM::icd_proc_2 as  icd_proc_2
,AM::icd_proc_3 as  icd_proc_3
,AM::icd_proc_4 as  icd_proc_4
,AM::icd_proc_5 as  icd_proc_5
,AM::icd_proc_6 as  icd_proc_6
,AM::icd_proc_7 as  icd_proc_7
,AM::icd_proc_8 as  icd_proc_8
,AM::icd_proc_9 as  icd_proc_9
,AM::icd_proc_10 as  icd_proc_10
,AM::icd_proc_11 as  icd_proc_11
,AM::icd_proc_12 as  icd_proc_12
,AM::icd_proc_13 as  icd_proc_13
,AM::icd_proc_14 as  icd_proc_14
,AM::icd_proc_15 as  icd_proc_15
,AM::icd_proc_16 as  icd_proc_16
,AM::icd_proc_17 as  icd_proc_17
,AM::icd_proc_18 as  icd_proc_18
,AM::icd_proc_19 as  icd_proc_19
,AM::icd_proc_20 as  icd_proc_20
,AM::icd_proc_21 as  icd_proc_21
,AM::icd_proc_22 as  icd_proc_22
,AM::icd_proc_23 as  icd_proc_23
,AM::icd_proc_24 as  icd_proc_24
,AM::icd_proc_25 as  icd_proc_25
,AM::UNITS as  UNITS
,AM::CHARGES as  CHARGES
,AM::ALLOWED as  ALLOWED
,AM::COPAY as  COPAY
,AM::COINS as  COINS
,AM::DEDUCT as  DEDUCT
,AM::COB as  COB
,AM::OTHREDUCT as  OTHREDUCT
,AM::PAID as  PAID;


AN3 = Join AN2 by (DRG, APR_GROUPER_KEY) LEFT OUTER, DRGCM3 by (drg_code, grouper_key) using 'replicated';

AN4 = Foreach AN3 Generate AN2::PAYER as PAYER
,AN2::ADMIT_KEY as  ADMIT_KEY
,AN2::ADMIT_DATE as  ADMIT_DATE
,AN2::MEMBER as  MEMBER
,AN2::PROVIDER as  PROVIDER
,AN2::ConceptID as  ConceptID
,AN2::LINES as  LINES
,AN2::DISCHARGE_DATE as  DISCHARGE_DATE
,AN2::MIN_DISCHARGE_STATUS as  MIN_DISCHARGE_STATUS
,AN2::MAX_DISCHARGE_STATUS as  MAX_DISCHARGE_STATUS
,AN2::GENDER as  GENDER
,AN2::LOB as  LOB
,AN2::DOB as  DOB
,AN2::BILL_TYPE as  BILL_TYPE
,AN2::PAR_CODE as  PAR_CODE
,AN2::DRG as  DRG
,AN2::APPROX_APR_WEIGHT as  APPROX_APR_WEIGHT
,AN2::DRG_TYPE as  DRG_TYPE
,AN2::MS_GROUPER_KEY as MS_GROUPER_KEY
,AN2::MS_drg_weight as MS_drg_weight
,AN2::MS_drg_gmlos as MS_drg_gmlos
,AN2::APR_GROUPER_KEY as APR_GROUPER_KEY
,DRGCM3::SOI1_drg_weight as APR1_drg_weight
,DRGCM3::SOI1_drg_gmlos as APR1_drg_gmlos
,DRGCM3::SOI2_drg_weight as APR2_drg_weight
,DRGCM3::SOI2_drg_gmlos as APR2_drg_gmlos
,DRGCM3::SOI3_drg_weight as APR3_drg_weight
,DRGCM3::SOI3_drg_gmlos as APR3_drg_gmlos
,DRGCM3::SOI4_drg_weight as APR4_drg_weight
,DRGCM3::SOI4_drg_gmlos as APR4_drg_gmlos
,AN2::AP_GROUPER_KEY as AP_GROUPER_KEY
,AN2::claim_diag_1 as  claim_diag_1
,AN2::claim_diag_2 as  claim_diag_2
,AN2::claim_diag_3 as  claim_diag_3
,AN2::claim_diag_4 as  claim_diag_4
,AN2::claim_diag_5 as  claim_diag_5
,AN2::claim_diag_6 as  claim_diag_6
,AN2::claim_diag_7 as  claim_diag_7
,AN2::claim_diag_8 as  claim_diag_8
,AN2::claim_diag_9 as  claim_diag_9
,AN2::claim_diag_10 as  claim_diag_10
,AN2::claim_diag_11 as  claim_diag_11
,AN2::claim_diag_12 as  claim_diag_12
,AN2::claim_diag_13 as  claim_diag_13
,AN2::claim_diag_14 as  claim_diag_14
,AN2::claim_diag_15 as  claim_diag_15
,AN2::claim_diag_16 as  claim_diag_16
,AN2::claim_diag_17 as  claim_diag_17
,AN2::claim_diag_18 as  claim_diag_18
,AN2::claim_diag_19 as  claim_diag_19
,AN2::claim_diag_20 as  claim_diag_20
,AN2::claim_diag_21 as  claim_diag_21
,AN2::claim_diag_22 as  claim_diag_22
,AN2::claim_diag_23 as  claim_diag_23
,AN2::claim_diag_24 as  claim_diag_24
,AN2::claim_diag_25 as  claim_diag_25
,AN2::e_diag_1 as  e_diag_1
,AN2::e_diag_2 as  e_diag_2
,AN2::e_diag_3 as  e_diag_3
,AN2::admit_diag as  admit_diag
,AN2::icd_proc_1 as  icd_proc_1
,AN2::icd_proc_2 as  icd_proc_2
,AN2::icd_proc_3 as  icd_proc_3
,AN2::icd_proc_4 as  icd_proc_4
,AN2::icd_proc_5 as  icd_proc_5
,AN2::icd_proc_6 as  icd_proc_6
,AN2::icd_proc_7 as  icd_proc_7
,AN2::icd_proc_8 as  icd_proc_8
,AN2::icd_proc_9 as  icd_proc_9
,AN2::icd_proc_10 as  icd_proc_10
,AN2::icd_proc_11 as  icd_proc_11
,AN2::icd_proc_12 as  icd_proc_12
,AN2::icd_proc_13 as  icd_proc_13
,AN2::icd_proc_14 as  icd_proc_14
,AN2::icd_proc_15 as  icd_proc_15
,AN2::icd_proc_16 as  icd_proc_16
,AN2::icd_proc_17 as  icd_proc_17
,AN2::icd_proc_18 as  icd_proc_18
,AN2::icd_proc_19 as  icd_proc_19
,AN2::icd_proc_20 as  icd_proc_20
,AN2::icd_proc_21 as  icd_proc_21
,AN2::icd_proc_22 as  icd_proc_22
,AN2::icd_proc_23 as  icd_proc_23
,AN2::icd_proc_24 as  icd_proc_24
,AN2::icd_proc_25 as  icd_proc_25
,AN2::UNITS as  UNITS
,AN2::CHARGES as  CHARGES
,AN2::ALLOWED as  ALLOWED
,AN2::COPAY as  COPAY
,AN2::COINS as  COINS
,AN2::DEDUCT as  DEDUCT
,AN2::COB as  COB
,AN2::OTHREDUCT as  OTHREDUCT
,AN2::PAID as  PAID;


AN5 = Join AN4 by (DRG, AP_GROUPER_KEY) LEFT OUTER, DRGCM3 by (drg_code, grouper_key) using 'replicated';

AN6 = Foreach AN5 Generate AN4::PAYER as PAYER
,AN4::ADMIT_KEY as  ADMIT_KEY
,AN4::ADMIT_DATE as  ADMIT_DATE
,AN4::MEMBER as  MEMBER
,AN4::PROVIDER as  PROVIDER
,AN4::ConceptID as  ConceptID
,AN4::LINES as  LINES
,AN4::DISCHARGE_DATE as  DISCHARGE_DATE
,AN4::MIN_DISCHARGE_STATUS as  MIN_DISCHARGE_STATUS
,AN4::MAX_DISCHARGE_STATUS as  MAX_DISCHARGE_STATUS
,AN4::GENDER as  GENDER
,AN4::LOB as  LOB
,AN4::DOB as  DOB
,AN4::BILL_TYPE as  BILL_TYPE
,AN4::PAR_CODE as  PAR_CODE
,AN4::DRG as  DRG
,AN4::APPROX_APR_WEIGHT as  APPROX_APR_WEIGHT
,AN4::DRG_TYPE as  DRG_TYPE
,AN4::MS_GROUPER_KEY as MS_GROUPER_KEY
,AN4::MS_drg_weight as MS_drg_weight
,AN4::MS_drg_gmlos as MS_drg_gmlos
,AN4::APR_GROUPER_KEY as APR_GROUPER_KEY
,AN4::APR1_drg_weight as APR1_drg_weight
,AN4::APR1_drg_gmlos as APR1_drg_gmlos
,AN4::APR2_drg_weight as APR2_drg_weight
,AN4::APR2_drg_gmlos as APR2_drg_gmlos
,AN4::APR3_drg_weight as APR3_drg_weight
,AN4::APR3_drg_gmlos as APR3_drg_gmlos
,AN4::APR4_drg_weight as APR4_drg_weight
,AN4::APR4_drg_gmlos as APR4_drg_gmlos
,AN4::AP_GROUPER_KEY as AP_GROUPER_KEY
,DRGCM3::SOI0_drg_weight as AP_drg_weight
,DRGCM3::SOI0_drg_gmlos as AP_drg_gmlos
,AN4::claim_diag_1 as  claim_diag_1
,AN4::claim_diag_2 as  claim_diag_2
,AN4::claim_diag_3 as  claim_diag_3
,AN4::claim_diag_4 as  claim_diag_4
,AN4::claim_diag_5 as  claim_diag_5
,AN4::claim_diag_6 as  claim_diag_6
,AN4::claim_diag_7 as  claim_diag_7
,AN4::claim_diag_8 as  claim_diag_8
,AN4::claim_diag_9 as  claim_diag_9
,AN4::claim_diag_10 as  claim_diag_10
,AN4::claim_diag_11 as  claim_diag_11
,AN4::claim_diag_12 as  claim_diag_12
,AN4::claim_diag_13 as  claim_diag_13
,AN4::claim_diag_14 as  claim_diag_14
,AN4::claim_diag_15 as  claim_diag_15
,AN4::claim_diag_16 as  claim_diag_16
,AN4::claim_diag_17 as  claim_diag_17
,AN4::claim_diag_18 as  claim_diag_18
,AN4::claim_diag_19 as  claim_diag_19
,AN4::claim_diag_20 as  claim_diag_20
,AN4::claim_diag_21 as  claim_diag_21
,AN4::claim_diag_22 as  claim_diag_22
,AN4::claim_diag_23 as  claim_diag_23
,AN4::claim_diag_24 as  claim_diag_24
,AN4::claim_diag_25 as  claim_diag_25
,AN4::e_diag_1 as  e_diag_1
,AN4::e_diag_2 as  e_diag_2
,AN4::e_diag_3 as  e_diag_3
,AN4::admit_diag as  admit_diag
,AN4::icd_proc_1 as  icd_proc_1
,AN4::icd_proc_2 as  icd_proc_2
,AN4::icd_proc_3 as  icd_proc_3
,AN4::icd_proc_4 as  icd_proc_4
,AN4::icd_proc_5 as  icd_proc_5
,AN4::icd_proc_6 as  icd_proc_6
,AN4::icd_proc_7 as  icd_proc_7
,AN4::icd_proc_8 as  icd_proc_8
,AN4::icd_proc_9 as  icd_proc_9
,AN4::icd_proc_10 as  icd_proc_10
,AN4::icd_proc_11 as  icd_proc_11
,AN4::icd_proc_12 as  icd_proc_12
,AN4::icd_proc_13 as  icd_proc_13
,AN4::icd_proc_14 as  icd_proc_14
,AN4::icd_proc_15 as  icd_proc_15
,AN4::icd_proc_16 as  icd_proc_16
,AN4::icd_proc_17 as  icd_proc_17
,AN4::icd_proc_18 as  icd_proc_18
,AN4::icd_proc_19 as  icd_proc_19
,AN4::icd_proc_20 as  icd_proc_20
,AN4::icd_proc_21 as  icd_proc_21
,AN4::icd_proc_22 as  icd_proc_22
,AN4::icd_proc_23 as  icd_proc_23
,AN4::icd_proc_24 as  icd_proc_24
,AN4::icd_proc_25 as  icd_proc_25
,AN4::UNITS as  UNITS
,AN4::CHARGES as  CHARGES
,AN4::ALLOWED as  ALLOWED
,AN4::COPAY as  COPAY
,AN4::COINS as  COINS
,AN4::DEDUCT as  DEDUCT
,AN4::COB as  COB
,AN4::OTHREDUCT as  OTHREDUCT
,AN4::PAID as  PAID;

AO = Foreach AN6 Generate CONCAT('_',PAYER) AS payer
,CONCAT('_',ADMIT_KEY) as admit_key
,CONCAT('_',SUBSTRING(ADMIT_DATE,4,6)) as admit_month
,CONCAT('_',MIN_DISCHARGE_STATUS) as min_discharge_status
,CONCAT('_',MAX_DISCHARGE_STATUS) as max_discharge_status
,CONCAT('_',GENDER) as gender
,CONCAT('_',LOB) as lob
,DOB as dob
,(DOB is null ? 0 : (DOB matches '^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$' ? DateDiff(ADMIT_DATE,DOB,365.25) : 0 )) as age

,((DISCHARGE_DATE is null ? 0 : (DISCHARGE_DATE matches '^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$' ? DateDiff(DISCHARGE_DATE,ADMIT_DATE) : 0)) <= 0 ? 1 : (DISCHARGE_DATE is null ? 0 : (DISCHARGE_DATE matches '^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$' ? DateDiff(DISCHARGE_DATE,ADMIT_DATE) : 0))) AS los

, CONCAT('_',BILL_TYPE) as bill_type
, CONCAT('_',PAR_CODE) as par_code
, (DRG is null ? '_no_data' : CONCAT('_',DRG)) as drg

, APPROX_APR_WEIGHT

,(DRG_TYPE == '_AP-DRG' ? DRG_TYPE : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR1_drg_weight is null ? (MS_drg_weight is null ? (AP_drg_weight is null ? '_no_data' : '_AP-DRG') : '_MS-DRG') : DRG_TYPE) : 
				(MS_drg_weight is null ? (AP_drg_weight is null ? '_no_data' : '_AP-DRG') :  '_MS-DRG'))) as DRG_TYPE

,(DRG_TYPE == '_AP-DRG' ? AP_drg_weight : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR1_drg_weight is null ? (MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) : MS_drg_weight) : APR1_drg_weight) :
				(MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) :  MS_drg_weight))) as drg1_weight

,(DRG_TYPE == '_AP-DRG' ? AP_drg_weight : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR2_drg_weight is null ? (MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) : MS_drg_weight) : APR2_drg_weight) :
				(MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) :  MS_drg_weight))) as drg2_weight

,(DRG_TYPE == '_AP-DRG' ? AP_drg_weight : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR3_drg_weight is null ? (MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) : MS_drg_weight) : APR3_drg_weight) :
				(MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) :  MS_drg_weight))) as drg3_weight

,(DRG_TYPE == '_AP-DRG' ? AP_drg_weight : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR4_drg_weight is null ? (MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) : MS_drg_weight) : APR4_drg_weight) :
				(MS_drg_weight is null ? (AP_drg_weight is null ? 1 : AP_drg_weight) :  MS_drg_weight))) as drg4_weight

,(DRG_TYPE == '_AP-DRG' ? AP_drg_gmlos : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR1_drg_gmlos is null ? (MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) : MS_drg_gmlos) : APR1_drg_gmlos) :
				(MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) :  MS_drg_gmlos))) as drg1_gmlos

,(DRG_TYPE == '_AP-DRG' ? AP_drg_gmlos : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR2_drg_gmlos is null ? (MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) : MS_drg_gmlos) : APR2_drg_gmlos) :
				(MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) :  MS_drg_gmlos))) as drg2_gmlos

,(DRG_TYPE == '_AP-DRG' ? AP_drg_gmlos : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR3_drg_gmlos is null ? (MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) : MS_drg_gmlos) : APR3_drg_gmlos) :
				(MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) :  MS_drg_gmlos))) as drg3_gmlos

,(DRG_TYPE == '_AP-DRG' ? AP_drg_gmlos : 
		(DRG_TYPE == '_APR-DRG' ? 
			(APR4_drg_gmlos is null ? (MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) : MS_drg_gmlos) : APR4_drg_gmlos) :
				(MS_drg_gmlos is null ? (AP_drg_gmlos is null ? 1 : AP_drg_gmlos) :  MS_drg_gmlos))) as drg4_gmlos

, CONCAT('_',ConceptID) as concept_id
, (claim_diag_1 matches '^_?0$' ? '_no_data' : (claim_diag_1 matches '_.*' ? claim_diag_1 : CONCAT('_',claim_diag_1))) AS diag1
, (claim_diag_2 matches '^_?0$' ? '_no_data' : (claim_diag_2 matches '_.*' ? claim_diag_2 : CONCAT('_',claim_diag_2))) AS diag2
, (claim_diag_3 matches '^_?0$' ? '_no_data' : (claim_diag_3 matches '_.*' ? claim_diag_3 : CONCAT('_',claim_diag_3))) AS diag3
, (claim_diag_4 matches '^_?0$' ? '_no_data' : (claim_diag_4 matches '_.*' ? claim_diag_4 : CONCAT('_',claim_diag_4))) AS diag4
, (claim_diag_5 matches '^_?0$' ? '_no_data' : (claim_diag_5 matches '_.*' ? claim_diag_5 : CONCAT('_',claim_diag_5))) AS diag5
, (claim_diag_6 matches '^_?0$' ? '_no_data' : (claim_diag_6 matches '_.*' ? claim_diag_6 : CONCAT('_',claim_diag_6))) AS diag6
, (claim_diag_7 matches '^_?0$' ? '_no_data' : (claim_diag_7 matches '_.*' ? claim_diag_7 : CONCAT('_',claim_diag_7))) AS diag7
, (claim_diag_8 matches '^_?0$' ? '_no_data' : (claim_diag_8 matches '_.*' ? claim_diag_8 : CONCAT('_',claim_diag_8))) AS diag8
, (claim_diag_9 matches '^_?0$' ? '_no_data' : (claim_diag_9 matches '_.*' ? claim_diag_9 : CONCAT('_',claim_diag_9))) AS diag9
, (claim_diag_10 matches '^_?0$' ? '_no_data' : (claim_diag_10 matches '_.*' ? claim_diag_10 : CONCAT('_',claim_diag_10))) AS diag10
, (claim_diag_11 matches '^_?0$' ? '_no_data' : (claim_diag_11 matches '_.*' ? claim_diag_11 : CONCAT('_',claim_diag_11))) AS diag11
, (claim_diag_12 matches '^_?0$' ? '_no_data' : (claim_diag_12 matches '_.*' ? claim_diag_12 : CONCAT('_',claim_diag_12))) AS diag12
, (claim_diag_13 matches '^_?0$' ? '_no_data' : (claim_diag_13 matches '_.*' ? claim_diag_13 : CONCAT('_',claim_diag_13))) AS diag13
, (claim_diag_14 matches '^_?0$' ? '_no_data' : (claim_diag_14 matches '_.*' ? claim_diag_14 : CONCAT('_',claim_diag_14))) AS diag14
, (claim_diag_15 matches '^_?0$' ? '_no_data' : (claim_diag_15 matches '_.*' ? claim_diag_15 : CONCAT('_',claim_diag_15))) AS diag15
, (claim_diag_16 matches '^_?0$' ? '_no_data' : (claim_diag_16 matches '_.*' ? claim_diag_16 : CONCAT('_',claim_diag_16))) AS diag16
, (claim_diag_17 matches '^_?0$' ? '_no_data' : (claim_diag_17 matches '_.*' ? claim_diag_17 : CONCAT('_',claim_diag_17))) AS diag17
, (claim_diag_18 matches '^_?0$' ? '_no_data' : (claim_diag_18 matches '_.*' ? claim_diag_18 : CONCAT('_',claim_diag_18))) AS diag18
, (claim_diag_19 matches '^_?0$' ? '_no_data' : (claim_diag_19 matches '_.*' ? claim_diag_19 : CONCAT('_',claim_diag_19))) AS diag19
, (claim_diag_20 matches '^_?0$' ? '_no_data' : (claim_diag_20 matches '_.*' ? claim_diag_20 : CONCAT('_',claim_diag_20))) AS diag20
, (claim_diag_21 matches '^_?0$' ? '_no_data' : (claim_diag_21 matches '_.*' ? claim_diag_21 : CONCAT('_',claim_diag_21))) AS diag21
, (claim_diag_22 matches '^_?0$' ? '_no_data' : (claim_diag_22 matches '_.*' ? claim_diag_22 : CONCAT('_',claim_diag_22))) AS diag22
, (claim_diag_23 matches '^_?0$' ? '_no_data' : (claim_diag_23 matches '_.*' ? claim_diag_23 : CONCAT('_',claim_diag_23))) AS diag23
, (claim_diag_24 matches '^_?0$' ? '_no_data' : (claim_diag_24 matches '_.*' ? claim_diag_24 : CONCAT('_',claim_diag_24))) AS diag24
, (claim_diag_25 matches '^_?0$' ? '_no_data' : (claim_diag_25 matches '_.*' ? claim_diag_25 : CONCAT('_',claim_diag_25))) AS diag25
, (e_diag_1 is null ? '_no_data' : (e_diag_1 matches '^_?0$' ? '_no_data' : (e_diag_1 matches '_.*' ? e_diag_1 : CONCAT('_',e_diag_1)))) AS e_diag_1
, (e_diag_2 is null ? '_no_data' : (e_diag_2 matches '^_?0$' ? '_no_data' : (e_diag_2 matches '_.*' ? e_diag_2 : CONCAT('_',e_diag_2)))) AS e_diag_2
, (e_diag_3 is null ? '_no_data' : (e_diag_3 matches '^_?0$' ? '_no_data' : (e_diag_3 matches '_.*' ? e_diag_3 : CONCAT('_',e_diag_3)))) AS e_diag_3
, (admit_diag matches '^_?0$' ? '_no_data' : (admit_diag matches '_.*' ? admit_diag : CONCAT('_',admit_diag))) AS diag_admit
, (icd_proc_1 matches '^_?0$' ? '_no_data' : (icd_proc_1 matches '_.*' ? icd_proc_1 : CONCAT('_',icd_proc_1))) AS icd_proc1
, (icd_proc_2 matches '^_?0$' ? '_no_data' : (icd_proc_2 matches '_.*' ? icd_proc_2 : CONCAT('_',icd_proc_2))) AS icd_proc2
, (icd_proc_3 matches '^_?0$' ? '_no_data' : (icd_proc_3 matches '_.*' ? icd_proc_3 : CONCAT('_',icd_proc_3))) AS icd_proc3
, (icd_proc_4 matches '^_?0$' ? '_no_data' : (icd_proc_4 matches '_.*' ? icd_proc_4 : CONCAT('_',icd_proc_4))) AS icd_proc4
, (icd_proc_5 matches '^_?0$' ? '_no_data' : (icd_proc_5 matches '_.*' ? icd_proc_5 : CONCAT('_',icd_proc_5))) AS icd_proc5
, (icd_proc_6 matches '^_?0$' ? '_no_data' : (icd_proc_6 matches '_.*' ? icd_proc_6 : CONCAT('_',icd_proc_6))) AS icd_proc6
, (icd_proc_7 matches '^_?0$' ? '_no_data' : (icd_proc_7 matches '_.*' ? icd_proc_7 : CONCAT('_',icd_proc_7))) AS icd_proc7
, (icd_proc_8 matches '^_?0$' ? '_no_data' : (icd_proc_8 matches '_.*' ? icd_proc_8 : CONCAT('_',icd_proc_8))) AS icd_proc8
, (icd_proc_9 matches '^_?0$' ? '_no_data' : (icd_proc_9 matches '_.*' ? icd_proc_9 : CONCAT('_',icd_proc_9))) AS icd_proc9
, (icd_proc_10 matches '^_?0$' ? '_no_data' : (icd_proc_10 matches '_.*' ? icd_proc_10 : CONCAT('_',icd_proc_10))) AS icd_proc10
, (icd_proc_11 matches '^_?0$' ? '_no_data' : (icd_proc_11 matches '_.*' ? icd_proc_11 : CONCAT('_',icd_proc_11))) AS icd_proc11
, (icd_proc_12 matches '^_?0$' ? '_no_data' : (icd_proc_12 matches '_.*' ? icd_proc_12 : CONCAT('_',icd_proc_12))) AS icd_proc12
, (icd_proc_13 matches '^_?0$' ? '_no_data' : (icd_proc_13 matches '_.*' ? icd_proc_13 : CONCAT('_',icd_proc_13))) AS icd_proc13
, (icd_proc_14 matches '^_?0$' ? '_no_data' : (icd_proc_14 matches '_.*' ? icd_proc_14 : CONCAT('_',icd_proc_14))) AS icd_proc14
, (icd_proc_15 matches '^_?0$' ? '_no_data' : (icd_proc_15 matches '_.*' ? icd_proc_15 : CONCAT('_',icd_proc_15))) AS icd_proc15
, (icd_proc_16 matches '^_?0$' ? '_no_data' : (icd_proc_16 matches '_.*' ? icd_proc_16 : CONCAT('_',icd_proc_16))) AS icd_proc16
, (icd_proc_17 matches '^_?0$' ? '_no_data' : (icd_proc_17 matches '_.*' ? icd_proc_17 : CONCAT('_',icd_proc_17))) AS icd_proc17
, (icd_proc_18 matches '^_?0$' ? '_no_data' : (icd_proc_18 matches '_.*' ? icd_proc_18 : CONCAT('_',icd_proc_18))) AS icd_proc18
, (icd_proc_19 matches '^_?0$' ? '_no_data' : (icd_proc_19 matches '_.*' ? icd_proc_19 : CONCAT('_',icd_proc_19))) AS icd_proc19
, (icd_proc_20 matches '^_?0$' ? '_no_data' : (icd_proc_20 matches '_.*' ? icd_proc_20 : CONCAT('_',icd_proc_20))) AS icd_proc20
, (icd_proc_21 matches '^_?0$' ? '_no_data' : (icd_proc_21 matches '_.*' ? icd_proc_21 : CONCAT('_',icd_proc_21))) AS icd_proc21
, (icd_proc_22 matches '^_?0$' ? '_no_data' : (icd_proc_22 matches '_.*' ? icd_proc_22 : CONCAT('_',icd_proc_22))) AS icd_proc22
, (icd_proc_23 matches '^_?0$' ? '_no_data' : (icd_proc_23 matches '_.*' ? icd_proc_23 : CONCAT('_',icd_proc_23))) AS icd_proc23
, (icd_proc_24 matches '^_?0$' ? '_no_data' : (icd_proc_24 matches '_.*' ? icd_proc_24 : CONCAT('_',icd_proc_24))) AS icd_proc24
, (icd_proc_25 matches '^_?0$' ? '_no_data' : (icd_proc_25 matches '_.*' ? icd_proc_25 : CONCAT('_',icd_proc_25))) AS icd_proc25
, CHARGES as claim_amt_billed
, ALLOWED as claim_amt_allowed
,(ALLOWED - COPAY - DEDUCT) as claim_amt_paid
,(CHARGES > 0 ? (ALLOWED - COPAY - DEDUCT)/CHARGES : 0) as paid_to_billed_ratio
, COB as cob;


AP = Foreach AO Generate payer
,CONCAT(admit_key,CONCAT('##',concept_id)) as admit_key
,admit_month
,min_discharge_status
,max_discharge_status
,gender
,lob
,dob
,age
,los
,bill_type
,par_code
,drg
,DRG_TYPE as drg_type

, (DRG_TYPE != '_APR-DRG' ? '_0' :
		(APPROX_APR_WEIGHT <  drg1_weight ? '_1' : 
			(APPROX_APR_WEIGHT < drg2_weight ? '_2' :
				(APPROX_APR_WEIGHT < drg3_weight ? '_3' : '_4')))) as soi

, (DRG_TYPE != '_APR-DRG' ? drg1_weight :
		(APPROX_APR_WEIGHT <  drg1_weight ? drg1_weight : 
			(APPROX_APR_WEIGHT < drg2_weight ? drg2_weight :
				(APPROX_APR_WEIGHT < drg3_weight ? drg3_weight : drg4_weight)))) as drg_weight

, (DRG_TYPE != '_APR-DRG' ? drg1_gmlos :
		(APPROX_APR_WEIGHT <  drg1_weight ? drg1_gmlos : 
			(APPROX_APR_WEIGHT < drg2_weight ? drg2_gmlos :
				(APPROX_APR_WEIGHT < drg3_weight ? drg3_gmlos : drg4_gmlos)))) as gmlos

,(diag1 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag1,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG1
,(diag2 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag2,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG2
,(diag3 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag3,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG3
,(diag4 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag4,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG4
,(diag5 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag5,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG5
,(diag6 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag6,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG6
,(diag7 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag7,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG7
,(diag8 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag8,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG8
,(diag9 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag9,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG9
,(diag10 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag10,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG10
,(diag11 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag11,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG11
,(diag12 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag12,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG12
,(diag13 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag13,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG13
,(diag14 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag14,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG14
,(diag15 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag15,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG15
,(diag16 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag16,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG16
,(diag17 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag17,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG17
,(diag18 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag18,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG18
,(diag19 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag19,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG19
,(diag20 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag20,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG20
,(diag21 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag21,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG21
,(diag22 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag22,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG22
,(diag23 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag23,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG23
,(diag24 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag24,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG24
,(diag25 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag25,'_','') matches '$APR_CC' ? 1 : 0)) as APR_CC_DIAG25
,(diag1 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag1,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG1
,(diag2 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag2,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG2
,(diag3 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag3,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG3
,(diag4 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag4,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG4
,(diag5 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag5,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG5
,(diag6 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag6,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG6
,(diag7 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag7,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG7
,(diag8 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag8,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG8
,(diag9 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag9,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG9
,(diag10 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag10,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG10
,(diag11 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag11,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG11
,(diag12 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag12,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG12
,(diag13 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag13,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG13
,(diag14 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag14,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG14
,(diag15 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag15,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG15
,(diag16 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag16,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG16
,(diag17 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag17,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG17
,(diag18 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag18,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG18
,(diag19 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag19,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG19
,(diag20 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag20,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG20
,(diag21 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag21,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG21
,(diag22 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag22,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG22
,(diag23 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag23,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG23
,(diag24 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag24,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG24
,(diag25 is null ? 0 : (DRG_TYPE matches '_APR-DRG' and REPLACE(diag25,'_','') matches '$APR_MCC' ? 1 : 0)) as APR_MCC_DIAG25
,(diag1 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag1,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG1
,(diag2 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag2,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG2
,(diag3 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag3,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG3
,(diag4 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag4,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG4
,(diag5 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag5,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG5
,(diag6 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag6,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG6
,(diag7 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag7,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG7
,(diag8 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag8,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG8
,(diag9 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag9,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG9
,(diag10 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag10,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG10
,(diag11 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag11,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG11
,(diag12 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag12,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG12
,(diag13 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag13,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG13
,(diag14 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag14,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG14
,(diag15 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag15,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG15
,(diag16 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag16,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG16
,(diag17 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag17,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG17
,(diag18 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag18,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG18
,(diag19 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag19,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG19
,(diag20 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag20,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG20
,(diag21 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag21,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG21
,(diag22 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag22,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG22
,(diag23 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag23,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG23
,(diag24 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag24,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG24
,(diag25 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag25,'_','') matches '$MS_CC' ? 1 : 0)) as MS_CC_DIAG25
,(diag1 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag1,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG1
,(diag2 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag2,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG2
,(diag3 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag3,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG3
,(diag4 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag4,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG4
,(diag5 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag5,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG5
,(diag6 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag6,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG6
,(diag7 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag7,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG7
,(diag8 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag8,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG8
,(diag9 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag9,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG9
,(diag10 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag10,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG10
,(diag11 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag11,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG11
,(diag12 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag12,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG12
,(diag13 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag13,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG13
,(diag14 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag14,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG14
,(diag15 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag15,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG15
,(diag16 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag16,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG16
,(diag17 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag17,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG17
,(diag18 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag18,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG18
,(diag19 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag19,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG19
,(diag20 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag20,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG20
,(diag21 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag21,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG21
,(diag22 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag22,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG22
,(diag23 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag23,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG23
,(diag24 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag24,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG24
,(diag25 is null ? 0 : (DRG_TYPE matches '_MS-DRG' and REPLACE(diag25,'_','') matches '$MS_MCC' ? 1 : 0)) as MS_MCC_DIAG25
,(diag1 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag1,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG1
,(diag2 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag2,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG2
,(diag3 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag3,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG3
,(diag4 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag4,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG4
,(diag5 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag5,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG5
,(diag6 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag6,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG6
,(diag7 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag7,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG7
,(diag8 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag8,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG8
,(diag9 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag9,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG9
,(diag10 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag10,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG10
,(diag11 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag11,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG11
,(diag12 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag12,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG12
,(diag13 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag13,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG13
,(diag14 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag14,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG14
,(diag15 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag15,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG15
,(diag16 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag16,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG16
,(diag17 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag17,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG17
,(diag18 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag18,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG18
,(diag19 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag19,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG19
,(diag20 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag20,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG20
,(diag21 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag21,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG21
,(diag22 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag22,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG22
,(diag23 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag23,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG23
,(diag24 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag24,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG24
,(diag25 is null ? 0 : (DRG_TYPE matches '_AP-DRG' and REPLACE(diag25,'_','') matches '$AP_MCC' ? 1 : 0)) as AP_MCC_DIAG25
,concept_id
,diag1
,diag2
,diag3
,diag4
,diag5
,diag6
,diag7
,diag8
,diag9
,diag10
,diag11
,diag12
,diag13
,diag14
,diag15
,diag16
,diag17
,diag18
,diag19
,diag20
,diag21
,diag22
,diag23
,diag24
,diag25
,e_diag_1
,e_diag_2
,e_diag_3
,diag_admit
,icd_proc1
,icd_proc2
,icd_proc3
,icd_proc4
,icd_proc5
,icd_proc6
,icd_proc7
,icd_proc8
,icd_proc9
,icd_proc10
,icd_proc11
,icd_proc12
,icd_proc13
,icd_proc14
,icd_proc15
,icd_proc16
,icd_proc17
,icd_proc18
,icd_proc19
,icd_proc20
,icd_proc21
,icd_proc22
,icd_proc23
,icd_proc24
,icd_proc25
,claim_amt_billed
,claim_amt_allowed
,claim_amt_paid
,paid_to_billed_ratio
,cob;

AQ = Foreach AP Generate payer
,admit_key
,admit_month
,min_discharge_status
,max_discharge_status
,gender
,lob
,dob
,age
,(los is null ? gmlos : (los < 0 ? gmlos : los)) as los
,bill_type
,(par_code is null ? '_no_data' : par_code) as par_code
,drg
,drg_type
,soi
,drg_weight
,gmlos
,(drg_type matches '_APR-DRG' ? APR_CC_DIAG1 + APR_CC_DIAG2 + APR_CC_DIAG3 + APR_CC_DIAG4 + APR_CC_DIAG5 + APR_CC_DIAG6 + APR_CC_DIAG7 + APR_CC_DIAG8 + APR_CC_DIAG9 + APR_CC_DIAG10 + APR_CC_DIAG11 + APR_CC_DIAG12 + APR_CC_DIAG13 + APR_CC_DIAG14 + APR_CC_DIAG15 + APR_CC_DIAG16 + APR_CC_DIAG17 + APR_CC_DIAG18 + APR_CC_DIAG19 + APR_CC_DIAG20 + APR_CC_DIAG21 + APR_CC_DIAG22 + APR_CC_DIAG23 + APR_CC_DIAG24 + APR_CC_DIAG25 : (drg_type matches '_MS-DRG' ? MS_CC_DIAG1 + MS_CC_DIAG2 + MS_CC_DIAG3 + MS_CC_DIAG4 + MS_CC_DIAG5 + MS_CC_DIAG6 + MS_CC_DIAG7 + MS_CC_DIAG8 + MS_CC_DIAG9 + MS_CC_DIAG10 + MS_CC_DIAG11 + MS_CC_DIAG12 + MS_CC_DIAG13 + MS_CC_DIAG14 + MS_CC_DIAG15 + MS_CC_DIAG16 + MS_CC_DIAG17 + MS_CC_DIAG18 + MS_CC_DIAG19 + MS_CC_DIAG20 + MS_CC_DIAG21 + MS_CC_DIAG22 + MS_CC_DIAG23 + MS_CC_DIAG24 + MS_CC_DIAG25 : 0)) as cc_cnt
,(drg_type matches '_APR-DRG' ? APR_MCC_DIAG1 + APR_MCC_DIAG2 + APR_MCC_DIAG3 + APR_MCC_DIAG4 + APR_MCC_DIAG5 + APR_MCC_DIAG6 + APR_MCC_DIAG7 + APR_MCC_DIAG8 + APR_MCC_DIAG9 + APR_MCC_DIAG10 + APR_MCC_DIAG11 + APR_MCC_DIAG12 + APR_MCC_DIAG13 + APR_MCC_DIAG14 + APR_MCC_DIAG15 + APR_MCC_DIAG16 + APR_MCC_DIAG17 + APR_MCC_DIAG18 + APR_MCC_DIAG19 + APR_MCC_DIAG20 + APR_MCC_DIAG21 + APR_MCC_DIAG22 + APR_MCC_DIAG23 + APR_MCC_DIAG24 + APR_MCC_DIAG25 : (drg_type matches '_MS-DRG' ? MS_MCC_DIAG1 + MS_MCC_DIAG2 + MS_MCC_DIAG3 + MS_MCC_DIAG4 + MS_MCC_DIAG5 + MS_MCC_DIAG6 + MS_MCC_DIAG7 + MS_MCC_DIAG8 + MS_MCC_DIAG9 + MS_MCC_DIAG10 + MS_MCC_DIAG11 + MS_MCC_DIAG12 + MS_MCC_DIAG13 + MS_MCC_DIAG14 + MS_MCC_DIAG15 + MS_MCC_DIAG16 + MS_MCC_DIAG17 + MS_MCC_DIAG18 + MS_MCC_DIAG19 + MS_MCC_DIAG20 + MS_MCC_DIAG21 + MS_MCC_DIAG22 + MS_MCC_DIAG23 + MS_MCC_DIAG24 + MS_MCC_DIAG25 : (drg_type matches '_AP-DRG' ? AP_MCC_DIAG1 + AP_MCC_DIAG2 + AP_MCC_DIAG3 + AP_MCC_DIAG4 + AP_MCC_DIAG5 + AP_MCC_DIAG6 + AP_MCC_DIAG7 + AP_MCC_DIAG8 + AP_MCC_DIAG9 + AP_MCC_DIAG10 + AP_MCC_DIAG11 + AP_MCC_DIAG12 + AP_MCC_DIAG13 + AP_MCC_DIAG14 + AP_MCC_DIAG15 + AP_MCC_DIAG16 + AP_MCC_DIAG17 + AP_MCC_DIAG18 + AP_MCC_DIAG19 + AP_MCC_DIAG20 + AP_MCC_DIAG21 + AP_MCC_DIAG22 + AP_MCC_DIAG23 + AP_MCC_DIAG24 + AP_MCC_DIAG25 : 0))) as mcc_cnt
,concept_id
,diag1
,diag2
,diag3
,diag4
,diag5
,diag6
,diag7
,diag8
,diag9
,diag10
,diag11
,diag12
,diag13
,diag14
,diag15
,diag16
,diag17
,diag18
,diag19
,diag20
,diag21
,diag22
,diag23
,diag24
,diag25
,e_diag_1
,e_diag_2
,e_diag_3
,diag_admit
,icd_proc1
,icd_proc2
,icd_proc3
,icd_proc4
,icd_proc5
,icd_proc6
,icd_proc7
,icd_proc8
,icd_proc9
,icd_proc10
,icd_proc11
,icd_proc12
,icd_proc13
,icd_proc14
,icd_proc15
,icd_proc16
,icd_proc17
,icd_proc18
,icd_proc19
,icd_proc20
,icd_proc21
,icd_proc22
,icd_proc23
,icd_proc24
,icd_proc25
,claim_amt_billed
,claim_amt_allowed
,claim_amt_paid
,paid_to_billed_ratio
,cob;


rmf $DRG_VALUE_ESTMATION_SCORE_SET_OUT_PATH
STORE AQ into '$DRG_VALUE_ESTMATION_SCORE_SET_OUT_PATH' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|', 'NO_MULTILINE', 'UNIX', 'WRITE_OUTPUT_HEADER'); 