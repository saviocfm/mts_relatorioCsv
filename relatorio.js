///
//  Obs:
//      - alterar a função query para retornar o valor necessario
//      - caso tenha usuario para rotina alterar configuraçoes do banco
///
const fs = require('fs')
const oracledb = require('oracledb');
const { config } = require('process');

const debugLevel = 1
const range_dia_relatorio = 60
const milissegundos_por_dia = 1000 * 60 * 60 * 24;
const appConfig = {
    outDir: "//fs010/transferencia/Savio",
    user: '',
    password: '',
    connectString: "",
    appName: "teste",
    cronHr: 12,
    relatorioLocal: true,
    relatorioRemoto: false
}


function formatarDatasQuery(dt) {
    
    let day = String(dt.getDate()).padStart(2, '0');
    let month = String(dt.getMonth() + 1).padStart(2, '0');
    let year = dt.getFullYear().toString()
    if (day.length < 2) {
        day = "0" + day.toString()
    } 
    if (month.length < 2) {
        month = "0" + month
    }
    return year + month + day
    
}

try {
    oracledb.initOracleClient({ libDir: './instantclient_21_3' });
    debug('encontrou client oracle')
} catch (err) {
    debug('oracle client não encontrado');
    debug(err);
    process.exit(1);
}


function debug(alerta, level = 1) {
    if (debugLevel >= level) {
        console.log(alerta)
    }
}


const query = (dt_ini, dt_fim) => {
    return `
    WITH tab AS                                          
    ( SELECT DISTINCT NOTA.CODFILEMP ,                 
                NOMFILEMP,                                       
                NOTA.NUMNOTFSC  AS NOTA,                         
                TO_CHAR(NOTA.DATIPRNOTFSC,'YYYYMMDD')            
                AS DAT_EMISSAO,                               
                NOTA.VLRNOTFSC,                                  
                CASE                                             
                WHEN T1.DATHRAENT IS NULL                      
                THEN '        '                                
                ELSE TO_CHAR(T1.DATHRAENT,'YYYYMMDD')          
                END AS DATENTREGA ,                              
                0   AS QTHRPRV ,                                 
                NOTA.VLRTOTICMSAI ,                              
                VLRICMSBTTBT ,                                   
                CODNATOPEFSC,                                    
                ' ' AS DUA_CODOCO ,                              
                0   AS qtd,                                      
                NUMRMNNOTFSC ,                                   
                CASE                                             
                WHEN T1.DATHRAENT IS NULL                      
                THEN '        '                                
                ELSE TO_CHAR(T1.DATHRAENT,'DD/MM/YYYY')        
                END AS DTTRK ,                                   
                NUMSEQPEDTMK ,                                   
                CASE                                             
                WHEN T1.CODMOTTRPITF IS NULL THEN ' '         
                ELSE T1.CODMOTTRPITF END AS CODMOTTRPITF,   
                ' ' AS da4_loja   ,                              
                CASE                                             
                WHEN T1.NOMCPETRP IS NULL THEN ' '            
                ELSE T1.NOMCPETRP END AS NOMTRANSP            
                FROM MRT.T0157665 NOTA, MRT.T0163321 CARGA,        
                MRT.T0112963 FIL ,                               
                MRT.MOVRTMPEDNOTFSC T1                           
                WHERE                                              
                NOTA.NUMRMNNOTFSC=CARGA.NUMRMNTRP                 
                AND codaglcrgrtz<>'SALDO'                          
                AND nota.numnotfsc>0                                           
                AND NOTA.CODEMP     >0                             
                AND NOTA.CODFILEMP  >0                             
                AND TO_CHAR(NOTA.DATIPRNOTFSC,'YYYYMMDD')          
                  BETWEEN `+ dt_ini + ` AND ` + dt_fim + `    
                  AND TIPDOCICPFATTRP='FAT'                          
                  AND T1.DATHRAENT IS NULL                           
                  AND CODNATOPEFSC                                   
                  IN(5102, 5403, 6102, 6108, 6403,5405)         
                  AND NOTA.CODFILEMP    = FIL.CODFILEMP              
                  AND T1.DATIPRNOTFSC   =NOTA.DATIPRNOTFSC           
                  AND T1.NUMNOTFSC      =NOTA.NUMNOTFSC              
                  AND T1.CODFILEMPMRTFAT=NOTA.CODFILEMP              
                  )                                                              
                  
                  SELECT  CODFILEMP,                                   
                  NOMFILEMP    ,NOTA   ,DAT_EMISSAO,   VLRNOTFSC,    
                  DATENTREGA,  QTHRPRV,    VLRTOTICMSAI,             
                  VLRICMSBTTBT,    CODNATOPEFSC,   DUA_CODOCO, 0,    
                  CASE                                               
                  WHEN T2.DATPRVENT IS NULL                        
                  THEN '          '                                
                  ELSE TO_CHAR(T2.DATPRVENT ,'DD/MM/YYYY')         
                  END AS DATPRVENT ,NUMRMNNOTFSC,  DTTRK  ,          
                  CODMOTTRPITF , ' ' AS da4_loja, NOMTRANSP          
                  FROM tab left join  MRT.T0127561 T2                  
                  on SUBSTR( NUMRMNNOTFSC,10,6)= CODCRG                
                  AND CODFILEMPMRTFAT             = T2.CODFILEMPMRTFAT 
                  AND T2.CODFILEMPMRTFAT          = CODFILEMP          
                  AND NUMSEQPEDTMK                = T2.NUMPED          
                  AND T2.CODEMP                   = CODEMP             
                  ORDER BY 4 ,  3 ,1
                  `}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))


async function main() {
    const data_atual = new Date()
    var ultima_execucao = fs.readFileSync('lastexec.txt').toString()
    if (ultima_execucao != formatarDatasQuery(data_atual)){
        run(data_atual)
        fs.writeFileSync('lastexec.txt', formatarDatasQuery(data_atual))
    } else {
        debug('ja rodou hj')
    }
    await delay(1000*60*60*appConfig.cronHr)
    ///rodar mais rapido para debug
    //    await delay(1000*60) 
    ///
    main()
}




async function run(data_atual) {
    let connection;

    try {
        debug('tentando se conectar ao banco...')
        connection = await oracledb.getConnection(
            {
                user: appConfig.user,
                password: appConfig.password,
                connectString: appConfig.connectString
            }
        )
        debug('sucesso ao se conectar ao banco')
        gerarArquivo(appConfig.appName+'.csv', query(formatarDatasQuery(new Date(data_atual.getTime() - range_dia_relatorio * milissegundos_por_dia)), formatarDatasQuery(data_atual)), connection,data_atual)
        //enviarRelatorios()



    } catch (e) {
        debug(e)
        debug('erro ao rodar aplicação')
    }
}


async function gerarArquivo(nome, query, connection,data_atual) {
    let result = await connection.execute(query)
    let csv = ''
    for (let i in result.metaData) {
        if (i != 0) {
            csv = csv = csv + ';' + result.metaData[i].name
        } else {
            csv = csv + result.metaData[i].name
        }
    }
    csv = csv + '\n'

    for (let i in result.rows) {
        for (let j in result.rows[i]) {
            if (j != 0) {
                if (j == 1) {
                    csv = csv + ';' + result.rows[i][j].toString()
                } else {
                    csv = csv + ';' + result.rows[i][j].toString().replace(".", ",")
                }
            } else {
                csv = csv + result.rows[i][j].toString().replace(".", ",")
            }
        }
        csv = csv + '\n'
    }
    if (appConfig.relatorioLocal) {
        fs.writeFile('./relatorios/' + nome, csv, (err) => {
            if (err) throw err;
        })
    }
    if( appConfig.relatorioRemoto){
        fs.writeFile(appConfig.outDir+nome,csv, (err) => {    
            if (err) throw err;
        })
    }    
    debug('O arquivo ' + nome + ' foi criado em '+formatarDatasQuery(data_atual)+" "+ data_atual.getHours()+":"+data_atual.getMinutes())
}

main()