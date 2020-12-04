-- Query 2: Lender Financing Adjustments
SELECT
	sub.[Date],
	sub.[Account_Description], 
	SUM(sub.[Total_Adjustment]) as [total_adjustment] 
	FROM 
	(
	SELECT 
	[SAP_Number],
	[Date],
	[Account_Number],
	[Account_Description],
	[Total_Adjustment], 
	[Storage_Adjustment], 
	[UMove_Adjustment], 
	[Storage_Split]
	FROM [SAP_Data].[dbo].[Lender_Financing_Adjustments]
	) sub 
	WHERE sub.[SAP_Number] in ('7000006807',
'7000007084',
'7000007557',
'7000007954',
'7000008605',
'7000009146',
'7000008951',
'7000007973',
'7000007154',
'7000007585',
'7000008175',
'7000008502',
'7000008228',
'7000007435',
'7000009345',
'7000006767',
'7000009080',
'7000007586',
'7000008225',
'7000009299',
'7000007997',
'7000007494',
'7000007452',
'7000007456',
'7000008804',
'7000008161',
'7000008193',
'7000006526',
'7000009300',
'7000006353',
'7000009425',
'7000009771',
'7000009521',
'7000009259',
'7000009258',
'7000009818',
'7000009608',
'7000009940',
'7000009664',
'7000009533',
'7000009939',
'7000007451',
'7000007325',
'7000008003',
'7000006826',
'7000007396',
'7000007028',
'7000006304',
'7000009029',
'7000007743',
'7000007542',
'7000008277',
'7000008351',
'7000007432',
'7000005902',
'7000008116',
'7000008645',
'7000009165',
'7000006771',
'7000009168',
'7000007509',
'7000006527',
'7000008888',
'7000008611',
'7000007784',
'7000006494',
'7000006884',
'7000008167',
'7000008220',
'7000007278',
'7000007828',
'7000007438',
'7000007556',
'7000008410',
'7000007341',
'7000006213',
'7000008269',
'7000008078',
'7000006122',
'7000008274',
'7000008276',
'7000007227',
'7000008227',
'7000006308',
'7000006125',
'7000007136',
'7000007738',
'7000006513',
'7000008297',
'7000006514',
'7000008950',
'7000008348',
'7000008838',
'7000008858',
'7000008163',
'7000007804',
'7000009182',
'7000007631',
'7000007345',
'7000007554',
'7000006785',
'7000007296',
'7000006772',
'7000007111',
'7000007257',
'7000007276',
'7000007581',
'7000007513',
'7000009116',
'7000007764',
'7000007934',
'7000007338',
'7000007342',
'7000008592',
'7000007883',
'7000008835',
'7000006120',
'7000009298',
'7000007740',
'7000008647',
'7000007269',
'7000007304',
'7000006766',
'7000008382',
'7000008123',
'7000007634',
'7000008272',
'7000007858',
'7000007081',
'7000007020',
'7000006820',
'7000006936',
'7000008367',
'7000008308',
'7000006395',
'7000007076',
'7000006346',
'7000008299',
'7000007305',
'7000008381',
'7000008470',
'7000007137',
'7000007492',
'7000008197',
'7000007275',
'7000008221',
'7000007683',
'7000007713',
'7000006573',
'7000009132',
'7000009005',
'7000007489',
'7000007365',
'7000008473',
'7000007309',
'7000008477',
'7000008024',
'7000008362',
'7000005871',
'7000008952',
'7000006254',
'7000007182',
'7000006378',
'7000008474',
'7000008968',
'7000006955',
'7000007539',
'7000007686',
'7000008519',
'7000008195',
'7000008648',
'7000008312',
'7000006235',
'7000008176',
'7000008309',
'7000006883',
'7000008682',
'7000007319',
'7000008588',
'7000008902',
'7000008229',
'7000008700',
'7000006621',
'7000007827',
'7000006377',
'7000005867',
'7000006127',
'7000008899',
'7000007627',
'7000009026',
'7000009334',
'7000006323',
'7000005895',
'7000008194',
'7000008901',
'7000007181',
'7000006474',
'7000006374',
'7000008711',
'7000008017',
'7000007220',
'7000008075',
'7000007562',
'7000006128',
'7000006552',
'7000008306',
'7000008305',
'7000006650',
'7000008279',
'7000007108',
'7000007587',
'7000007563',
'7000007029',
'7000006399',
'7000009277',
'7000008162',
'7000008788',
'7000006371',
'7000008224',
'7000005898',
'7000009335',
'7000007931',
'7000006994',
'7000007716',
'7000007507',
'7000007515',
'7000008937',
'7000008787',
'7000007512',
'7000007242',
'7000005865',
'7000008166',
'7000007245',
'7000006881',
'7000006216',
'7000006680',
'7000005869',
'7000006454',
'7000008706',
'7000008903',
'7000006879',
'7000007508',
'7000008469',
'7000008188',
'7000006704',
'7000008331',
'7000007768',
'7000008665',
'7000007243',
'7000009336',
'7000007343',
'7000006956',
'7000007511',
'7000009028',
'7000005896',
'7000007344',
'7000006934',
'7000006824',
'7000006379',
'7000005899',
'7000006197',
'7000007241',
'7000007830',
'7000007223',
'7000009051',
'7000007247',
'7000009184',
'7000009084',
'7000007584',
'7000008900',
'7000009081',
'7000007658',
'7000008606',
'7000007248',
'7000005864',
'7000007681',
'7000007632',
'7000007493',
'7000006192',
'7000007687',
'7000008072',
'7000006904',
'7000007377',
'7000007419',
'7000008385',
'7000007272',
'7000007416',
'7000008301',
'7000006324',
'7000006217',
'7000008187',
'7000008505',
'7000005840',
'7000009346',
'7000006825',
'7000008556',
'7000007887',
'7000008557',
'7000008889',
'7000007714',
'7000008651',
'7000006356',
'7000006394',
'7000005847',
'7000008340',
'7000007360',
'7000008141',
'7000008389',
'7000006754',
'7000006729',
'7000005822',
'7000009312',
'7000008710',
'7000008394',
'7000008898',
'7000006728',
'7000007929',
'7000007888',
'7000008138',
'7000007361',
'7000008144',
'7000008769',
'7000008413',
'7000008708',
'7000006753',
'7000008683',
'7000006752',
'7000005844',
'7000007612',
'7000008046',
'7000007974',
'7000008491',
'7000007803',
'7000008860',
'7000008919',
'7000008506',
'7000008563',
'7000007075',
'7000006414',
'7000007656',
'7000006355',
'7000008077',
'7000006910',
'7000006307',
'7000007364',
'7000008254',
'7000007800',
'7000007657',
'7000006906',
'7000008981',
'7000008977',
'7000008094',
'7000008390',
'7000006576',
'7000008391',
'7000008870',
'7000007582',
'7000006907',
'7000007957',
'7000008448',
'7000008607',
'7000006903',
'7000008980',
'7000008095',
'7000009292',
'7000008716',
'7000007431',
'7000007135',
'7000005848',
'7000008245',
'7000009344',
'7000006810',
'7000008119',
'7000007933',
'7000007157',
'7000007274',
'7000007682',
'7000007633',
'7000009059',
'7000008504',
'7000008251',
'7000008721',
'7000008313',
'7000007277',
'7000007498',
'7000009099',
'7000006157',
'7000006937',
'7000007280',
'7000005897',
'7000007155',
'7000008801',
'7000008802',
'7000007471',
'7000007363',
'7000007711',
'7000009169',
'7000007956',
'7000005842',
'7000009052',
'7000009083',
'7000007104',
'7000006811',
'7000006232',
'7000007083',
'7000009053',
'7000008498',
'7000007198',
'7000007998',
'7000006159',
'7000006979',
'7000008252',
'7000007541',
'7000007071',
'7000006995',
'7000007996',
'7000009128',
'7000007246',
'7000008345',
'7000009004',
'7000006723',
'7000007437',
'7000009353',
'7000008797',
'7000007849',
'7000006828',
'7000008517',
'7000009517',
'7000008661',
'7000009163',
'7000009254',
'7000009760',
'7000009200',
'7000007761',
'7000009650',
'7000009807',
'7000009804',
'7000009497',
'7000009498',
'7000009645',
'7000009659',
'7000007819',
'7000006901',
'7000009506',
'7000006698',
'7000009507',
'7000009508',
'7000007978',
'7000009522',
'7000009523',
'7000009524',
'7000009525',
'7000009201',
'7000009526',
'7000009202',
'7000009528',
'7000009529',
'7000007760',
'7000007016',
'7000006269',
'7000006747',
'7000009535',
'7000009527',
'7000009534',
'7000009502',
'7000008407',
'7000008406',
'7000009518',
'7000008991',
'7000009514',
'7000009519',
'7000008973',
'7000009619',
'7000006169',
'7000006190',
'7000009520',
'7000006411',
'7000009499',
'7000009815',
'7000009257',
'7000009240',
'7000009217',
'7000009772',
'7000009494',
'7000008461',
'7000009776',
'7000008699',
'7000009797',
'7000009788',
'7000009800',
'7000009509',
'7000006970',
'7000010367',
'7000010323',
'7000006972',
'7000010425',
'7000010440',
'7000010540',
'7000010511',
'7000010595',
'7000010577',
'7000010589',
'7000010621',
'7000010632',
'7000007334',
'7000006616',
'7000008242',
'7000006365',
'7000007302',
'7000010648',
'7000010671',
'7000010670',
'7000010669',
'7000010672',
'7000010675',
'7000010761',
'7000010741',
'7000010742',
'7000010790',
'7000010848',
'7000006272',
'7000010875',
'7000011031',
'7000011032',
'7000011044',
'7000011037',
'7000011096',
'7000011103',
'7000011152',
'7000011206',
'7000011349',
'7000011350',
'7000011352',
'7000011383',
'7000011386',
'7000011390',
'7000011422')
GROUP BY sub.[Date] ,sub.[Account_Description]
ORDER BY Date ASC