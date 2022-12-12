DROP TABLE TECNOLOGIA.TB_BULKINSERT_TEST;

CREATE TABLE TECNOLOGIA.TB_BULKINSERT_TEST (
	IDBULK INT NOT NULL,
	NM_NOME VARCHAR2(200) NOT NULL,
	NM_ENDERECO VARCHAR2(500) NOT NULL,	
    DTREGISTRO DATE DEFAULT SYSDATE NOT NULL,
	CONSTRAINT TB_BULKINSERT_TEST_PK PRIMARY KEY (IDBULK) USING INDEX TABLESPACE TBS_TECNOLOGIA_I
);


--DELETE FROM TECNOLOGIA.TB_BULKINSERT_TEST;
--SELECT count(*) FROM TECNOLOGIA.TB_BULKINSERT_TEST;