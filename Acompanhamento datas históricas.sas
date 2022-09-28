libname RMBACNF "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/landing_area/configurations/rmbalm5.1.2_eba_281_201906/rd_conf";
libname rmbarslt "/install/SASConfig/Lev1/AppData/SASIRM/pa/data/380877679/rmbarslt";
libname gamma "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/input_area/configurations/rmbalm5.1.2_eba_281_201906/mapping";
libname rmbastg "/install/SASConfig/Lev1/AppData/SASIRM/pa/fas/fa.sbrlus/input_area/07312019";
libname rmbantmp "/install/SASConfig/Lev1/AppData/SASIRM/pa/data/380877679/rmbantmp";

DATA TESTE12;
SET rmbantmp.risk_factor_x_risk_fctr_curve;
RUN;
/**************INICIO RELs 2.2.3.2 - 2.2.3.3 - 2.2.3.4 - 2.2.4.2*****************/
proc sql;
create table uniaocurva as
	select
		a.RISK_FACTOR_ID,
		a.RISK_FACTOR_CATEGORY_NM,
		b.CURVE_ID
	from rmbastg.RISK_FACTOR as a
	left join rmbantmp.risk_factor_x_risk_fctr_curve as b
	on a.RISK_FACTOR_ID = b.RISK_FACTOR_ID
;quit;



/* proc sql; */
/* create table uniaocurva as */
/* 	select  */
/* 		a.RISK_FACTOR_ID, */
/* 		a.RISK_FACTOR_CATEGORY_NM, */
/* 		a.MATURITY_LENGTH_OF_TIME, */
/* 		a.CURVE_ID, */
/* 		c.x_br_day_basis_cd, */
/* 		input(compress(x_br_day_basis_cd,'','kd'),8.) as DU */
/* 	from uniaocurva1 as a */
/* 	left join rmbastg.risk_factor_curve as c */
/* 	on a.CURVE_ID = c.CURVE_ID */
/* ;quit; */

/* proc sql; */
/* create table teste7 as */
/* 	select distinct x_br_day_basis_cd, */
/* 	CURVE_ID */
/* from uniãocurvafim; */
/* ;quit; */

/********************ALLPRICE***********************/
/*************Criação tabela vazia****************/
data allprice_inicial1;
format BaseDate NLDATE20.
VERTICE 10.
AnalysisName $32. 
PRIMITIVE_RF $32.
x_br_cfdisc NLNUM16.2;
stop;
run;
/* ****************Fim tabela vazia************ */

data nevscen_analysis_option;
set rmbastg.nevscen_analysis_option(where=(CONFIG_NAME = 'TIMEGRID'));
call symputx('CONFIG_VALUE',CONFIG_VALUE);
run;

proc sql;
select max(time_bucket_seq_nbr) into :max from rmbacnf.time_grid_bucket where TIME_GRID_ID="&CONFIG_VALUE.";
quit;

/* %put &CONFIG_VALUE.; */
/* %put &max.; */

%macro vertices;

%let i=0;
%do i = 1 %to &max.;

proc sql;
select time_bucket_end_uom_no into: time_bucket_end_uom_no
from rmbacnf.time_grid_bucket
where time_bucket_seq_nbr=&i and TIME_GRID_ID="&CONFIG_VALUE.";
quit;

data custom_ALLPRICE(keep= BaseDate AnalysisName VERTICE PRIMITIVE_RF x_br_cfdisc);
set rmbarslt.ALLPRICE;
where PRIMITIVE_RF not is missing; /***Validar possibilidade de exclusão***/
VERTICE=&time_bucket_end_uom_no;
x_br_cfdisc=X_BR_CFDISC_&i;
rename _date_ = BaseDate;
run;

proc  append base=allprice_inicial1  data=custom_ALLPRICE force nowarn;
run;

%end;

%mend vertices;

%vertices;

proc sql;
create table allprice_inicial as
	select 
	BaseDate,
	VERTICE,
	AnalysisName,
	PRIMITIVE_RF,
	sum(x_br_cfdisc) as x_br_cfdisc
 	from allprice_inicial1
 group by BaseDate,	VERTICE, AnalysisName, PRIMITIVE_RF
 ;quit;

/*=-=-=-=-=-=-= UNIÃO STATES_A e STATES_V=-=-=-=--=-*/
proc sql;
create table teste2 as
select time_bucket_end_uom_no,TIME_GRID_ID
from rmbacnf.time_grid_bucket
where TIME_GRID_ID = "&CONFIG_VALUE."; 
quit;

data teste3;
set teste2;
by TIME_GRID_ID;
length combined $100.;
retain combined;

if first.TIME_GRID_ID then
combined=time_bucket_end_uom_no;
else
combined=catx(' ', combined, time_bucket_end_uom_no);

if last.TIME_GRID_ID then
output;
run;

proc sql;
select combined into :VERTICE from teste3;
quit;
%put &VERTICE.;

data STATES_A;
set rmbarslt.STATES_A;
run;

data STATES_V;
set rmbarslt.STATES_V;
run;

proc transpose data=STATES_V out=STATES_V1 (drop=_label_ rename=(col1=VALUE));
	by statenumber analysisnumber analysispart;
run;

data STATES_V2;
set STATES_V1;
VERTICE = input(compress(scan(tranwrd(_NAME_,"_"," "),-1),'','kd'),8.);
run;

proc sql;						
	create table STATES_V3 as 						
	select						
		a.statenumber,					
		a.analysisnumber,					
		a.analysispart,
		a._name_,
		a.VERTICE,	
		a.VALUE,					
		b.analysisname					
	from STATES_V2 as a left join STATES_A as b						
		on a.analysisnumber = b.analysisnumber					
	WHERE						
		A.VERTICE IN (&VERTICE.);				
;quit;

proc sql;
	create table STATES_FIM as
	select *
/* 		b.MATURITY_LENGTH_OF_TIME * b.DU as VERTICE, */
/* 		scan(tranwrd(_NAME_,"_"," "),-1) as VERTICE2, */
/* 		input(compress(scan(tranwrd(_NAME_,"_"," "),-1),'','kd'),8.) as VERTICE3  */
	from STATES_V3 as a
	left join uniaocurva as b
	on a._name_= b.RISK_FACTOR_ID
;quit;

/* proc sql; */
/* create table teste7 as */
/* 	select distinct VERTICE, */
/* 	_NAME_, */
/* 	DU, */
/* 	VERTICE2, */
/* 	VERTICE3 */
/* from STATES_FIM */
/* where VERTICE not is missing and VERTICE <> VERTICE3 */
/* ;quit; */

/* proc sql; */
/* create table teste8 as */
/* 	select distinct VERTICE, */
/* 	VERTICE3 */
/* from teste7 */
/* where VERTICE not is missing and VERTICE <> VERTICE3 */
/* ;quit; */

/****************ALTERAR NOME ANTES DE SUBIR****************/
/**************UNIÃO TABELA FINAL************/
proc sql;						
	create table TABELA_FIM1 as 						
	select
		a.BaseDate,
		a.VERTICE,
		a.PRIMITIVE_RF,
		a.x_br_cfdisc, 
		a.AnalysisName,
		b._NAME_,
		b.VALUE as Value_states,
		b.RISK_FACTOR_ID,
		b.CURVE_ID,
		b.RISK_FACTOR_CATEGORY_NM
	from allprice_inicial as a 
	left join STATES_FIM as b					
	on a.AnalysisName = b.AnalysisName
	and a.VERTICE = b.VERTICE
	and a.PRIMITIVE_RF = b.RISK_FACTOR_CATEGORY_NM;
	;quit;

/* data tabelafim2; */
/* set TABELA_FIM1; */
/* where AnalysisName not contains 'HISTORICO' */
/* and RISK_FACTOR_CATEGORY_NM not is missing; */
/* run; */

data basecase notbasecase;
set TABELA_FIM1;
where RISK_FACTOR_CATEGORY_NM not is missing;
if AnalysisName = 'BASECASE' then output basecase;
else output notbasecase;
run;

data nevscen_analysis_option1;
set rmbastg.nevscen_analysis_option(where=(CONFIG_NAME = 'RED_TYPE'));
call symputx('RED_TYPE',CONFIG_VALUE);
run;
	

proc sql;
	create table tabela_fim as
select a.*,
	   b.Value_states as Value_states_bc,
	   b.x_br_cfdisc as x_br_cfdisc_bc
from notbasecase as a
left join basecase as b
on a.BaseDate = b.BaseDate
and a.VERTICE = b.VERTICE
and a.PRIMITIVE_RF = b.PRIMITIVE_RF
and a._NAME_ = b._NAME_
and	a.RISK_FACTOR_ID = b.RISK_FACTOR_ID
and a.CURVE_ID = b.CURVE_ID
where prxmatch("/^.+_R&RED_TYPE._[0-9]+$/i", trim(a.risk_factor_id))
;quit;




/**************FIM RELs 2.2.3.2 - 2.2.3.3 - 2.2.3.4 - 2.2.4.2*****************/
/*-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=*/



