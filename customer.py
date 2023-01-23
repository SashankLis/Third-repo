import Snowflake as sf


def customer_load():

    #Customer

    Truncate temp table customer

    truncate_tmp_customer = '''
                                      TRUNCATE TABLE BHATBHATENI.SASHANK_TMP.TMP_CUSTOMER
    
                                       '''
    
    sf.execute_query(truncate_tmp_customer)


    #Load tmp table customer

    load_tmp_customer = '''
                INSERT INTO BHATBHATENI.SASHANK_TMP.TMP_CUSTOMER(
                CUSTOMER_ID,CUSTOMER_FST_NM,
                CUSTOMER_MID_NM,CUSTOMER_LST_NM,
                CUSTOMER_ADDR
                )
                SELECT ID,CUSTOMER_FIRST_NAME,CUSTOMER_MIDDLE_NAME,
                CUSTOMER_LAST_NAME,CUSTOMER_ADDRESS
                FROM BHATBHATENI.SASHANK_STG.STG_CUSTOMER;
           '''
    
    sf.execute_query(load_tmp_customer)

    #Load data to target table customer

    load_tgt_customer = '''
                       INSERT INTO BHATBHATENI.SASHANK_TGT.TGT_CUSTOMER(
                       CUSTOMER_KY,CUSTOMER_ID,CUSTOMER_FST_NM,CUSTOMER_MID_NM,
                       CUSTOMER_LST_NM,CUSTOMER_ADDR,OPEN_CLOSE_CD,ROW_INSRT_TMS,
                       ROW_UPDT_TMS
                       )
                       SELECT CUSTOMER_KY,CUSTOMER_ID,CUSTOMER_FST_NM,CUSTOMER_MID_NM,CUSTOMER_LST_NM,CUSTOMER_ADDR,
                       1,LOCALTIMESTAMP,LOCALTIMESTAMP
                       FROM BHATBHATENI.SASHANK_TMP.TMP_CUSTOMER
                       WHERE CUSTOMER_ID NOT IN (SELECT DISTINCT CUSTOMER_ID FROM BHATBHATENI.SASHANK_TGT.TGT_CUSTOMER);
                        '''
    sf.execute_query(load_tgt_customer)

    # Update target table customer

    update_tgt_customer = '''
                                       UPDATE BHATBHATENI.SASHANK_TGT.TGT_CUSTOMER AS T1
                                       SET T1.CUSTOMER_FST_NM = T2.CUSTOMER_FST_NM,
                                       T1.CUSTOMER_MID_NM = T2.CUSTOMER_MID_NM,
                                       T1.CUSTOMER_LST_NM = T2.CUSTOMER_LST_NM,
                                       T1.CUSTOMER_ADDR = T2.CUSTOMER_ADDR,
                                       ROW_UPDT_TMS = LOCALTIMESTAMP
                                       FROM BHATBHATENI.SASHANK_TMP.TMP_CUSTOMER T2
                                       WHERE T1.CUSTOMER_ID = T2.CUSTOMER_ID;
                                       '''
    sf.execute_query(update_tgt_customer)