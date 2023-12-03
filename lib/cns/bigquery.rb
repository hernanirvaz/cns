# frozen_string_literal: true

require('google/cloud/bigquery')
require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  BD = 'hernanirvaz.coins'

  # classe para processar bigquery
  class Bigquery
    # @return [Google::Cloud::Bigquery] API bigquery
    attr_reader :api
    # @return [Google::Cloud::Bigquery::QueryJob] job bigquery
    attr_reader :job
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops
    # @return (see sql)
    attr_reader :sqr

    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra transacoes trades & ledger?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Bigquery] API bigquery
    def initialize(pop)
      # usa env GOOGLE_APPLICATION_CREDENTIALS para obter credentials
      # @see https://cloud.google.com/bigquery/docs/authentication/getting-started
      @api = Google::Cloud::Bigquery.new
      @ops = pop
    end

    # mostra situacao completa entre kraken/bitcoinde/paymium/therock/etherscan/greymass/beaconchain & bigquery
    def mostra_tudo
      apius.mostra_resumo
      apide.mostra_resumo
      #apifr.mostra_resumo
      #apimt.mostra_resumo
      apies.mostra_resumo
      apigm.mostra_resumo
      #apibc.mostra_resumo
    end

    # mostra situacao completa entre kraken/etherscan & bigquery
    def mostra_skrk
      apius.mostra_resumo
      apies.mostra_resumo
    end

    # mostra situacao completa entre etherscan & bigquery
    def mostra_seth
      apies.mostra_resumo
    end

    # insere (caso existam) dados novos kraken/bitcoinde/paymium/therock/etherscan/greymass/beaconchain no bigquery
    def processa_tudo
      puts(Time.now.strftime("TRANSACOES  %Y-%m-%d %H:%M ") + processa_us + ", " + processa_de + ", " + processa_eth + ", " + processa_eos)
    end

    # insere (caso existam) dados novos kraken/etherscan no bigquery
    def processa_wkrk
      puts(Time.now.strftime("TRANSACOES  %Y-%m-%d %H:%M ") + processa_us + ", " + processa_eth)
    end

    # insere (caso existam) dados novos etherscan no bigquery
    def processa_weth
      puts(Time.now.strftime("TRANSACOES  %Y-%m-%d %H:%M ") + processa_eth)
    end

    private

    # insere transacoes blockchain novas nas tabelas etht (norml), ethi (internas), ethp (block), ethw (withdrawals), ethk (token)
    def processa_eth
      str = "ETH"
      str << format(" %<n>i etht", n: dml(etht_ins)) if apies.novtx.count > 0
      str << format(" %<n>i ethi", n: dml(ethi_ins)) if apies.novix.count > 0
      str << format(" %<n>i ethp", n: dml(ethp_ins)) if apies.novpx.count > 0
      str << format(" %<n>i ethw", n: dml(ethw_ins)) if apies.novwx.count > 0
      str << format(" %<n>i ethk", n: dml(ethk_ins)) if apies.novkx.count > 0
      str
    end

    # insere transacoes exchange kraken novas nas tabelas ust (trades), usl (ledger)
    def processa_us
      str = "KRAKEN"
      str << format(" %<n>i ust", n: dml(ust_ins)) if apius.trades.count > 0
      str << format(" %<n>i usl", n: dml(usl_ins)) if apius.ledger.count > 0
      str
    end

    # insere transacoes exchange bitcoinde novas nas tabelas det (trades), del (ledger)
    def processa_de
      str = "BITCOINDE"
      str << format(" %<n>i det", n: dml(det_ins)) if apide.trades.count > 0
      str << format(" %<n>i del", n: dml(del_ins)) if apide.ledger.count > 0
      str
    end

    # insere transacoes blockchain novas na tabela eos
    def processa_eos
      str = "EOS"
      str << format(" %<n>i eos ", n: dml(eost_ins)) if apigm.novax.count > 0
      str
    end

    # insere transacoes exchange paymium/therock  novas na tabela fr/mt (ledger)
    # def processa_frmt
    #   puts(format("%<n>4i LEDGER\tPAYMIUM\t\tINSERIDAS fr", n: apifr.ledger.empty? ? 0 : dml(frl_ins)))
    #   puts(format("%<n>4i LEDGER\tTHEROCK\t\tINSERIDAS mt", n: apimt.ledger.empty? ? 0 : dml(mtl_ins)))
    # end
    # insere historico sados novos na tabela eth2bh
    # def processa_bc
    #   puts(format("%<n>4i ATTESTATIONS INSERIDAS eth2at", n: apibc.novtx.empty? ? 0 : dml(eth2at_ins)))
    #   puts(format("%<n>4i PROPOSALS INSERIDAS eth2pr", n: apibc.novkx.empty? ? 0 : dml(eth2pr_ins)))
    #   puts(format("%<n>4i BALANCES\tETH2\t\tINSERIDOS eth2bh", n: apibc.nov.empty? ? 0 : dml(eth2bh_ins)))
    # end

    # cria job bigquery & verifica execucao
    #
    # @param cmd (see sql)
    # @return [Boolean] job ok?
    def job?(cmd)
      @job = api.query_job(cmd)
      job.wait_until_done!
      fld = job.failed?
      puts(job.error['message']) if fld
      fld
    end

    # cria Structured Query Language (SQL) job bigquery
    #
    # @param [String] cmd comando SQL a executar
    # @param [String] res resultado quando SQL tem erro
    # @return [Google::Cloud::Bigquery::Data] resultado do SQL
    def sql(cmd, res = [])
      @sqr = job?(cmd) ? res : job.data
    end

    # cria Data Manipulation Language (DML) job bigquery
    #
    # @param cmd (see sql)
    # @return [Integer] numero linhas afetadas
    def dml(cmd)
      job?(cmd) ? 0 : job.num_dml_affected_rows
    end

    # @return [String] comando insert SQL formatado fr (ledger)
    def mtl_ins
      "insert #{BD}.mt(id,time,type,valor,moe,pair,note,trade_id,dias) " \
      "VALUES#{apimt.ledger.map { |obj| mtl_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado eth2bh
    def eth2bh_ins
      "insert #{BD}.eth2bh(balance,effectivebalance,epoch,validatorindex" \
        ") VALUES#{apibc.nov[0..1000].map { |obj| eth2bh_1val(obj) }.join(',')}"
    end

    # @return [Etherscan] API blockchain ETH
    def apies
      @apies ||= Etherscan.new(
        {
          wb: sql("select * from #{BD}.walletEth order by 2"),
          nt: sql("select itx,iax from #{BD}.ethtx"),
          ni: sql("select itx,iax from #{BD}.ethix"),
          np: sql("select itx,iax from #{BD}.ethpx"),
          nw: sql("select itx,iax from #{BD}.ethwx"),
          nk: sql("select itx,iax from #{BD}.ethkx")
        },
        ops
      )
    end

    # @return [Greymass] API blockchain EOS
    def apigm
      @apigm ||= Greymass.new(
        {
          wb: sql("select * from #{BD}.walletEos order by 2"),
          nt: sql("select itx,iax from #{BD}.eostx")
        },
        ops
      )
    end

    # @return [Beaconchain] API blockchain ETH2
    def apibc
      @apibc ||= Beaconchain.new(
        {
          wb: sql("select * from #{BD}.walletEth2 order by 1"),
          nb: sql("select itx,iax from #{BD}.eth2bhx")
        },
        ops
      )
    end

    # @return [Kraken] API exchange kraken
    def apius
      @apius ||= Kraken.new(
        {
          sl: sql("select sum(btc) xxbt,sum(eth) xeth,sum(eos) eos,sum(eur) zeur from #{BD}.ussl")[0],
          nt: sql("select * from #{BD}.ustx order by time,txid"),
          nl: sql("select * from #{BD}.uslx order by time,txid")
        },
        ops
      )
    end

    # @return [Bitcoinde] API exchange bitcoinde
    def apide
      @apide ||= Bitcoinde.new(
        {
          sl: sql("select sum(btc) btc from #{BD}.desl")[0],
          nt: sql("select * from #{BD}.detx order by time,txid"),
          nl: sql("select * from #{BD}.delx order by time,txid")
        },
        ops
      )
    end

    # @return [Paymium] API exchange paymium
    def apifr
      @apifr ||= Paymium.new(
        {
          sl: sql("select sum(btc) btc,sum(eur) eur from #{BD}.frsl")[0],
          nl: sql("select * from #{BD}.frlx order by time,txid")
        },
        ops
      )
    end

    # @return [TheRock] API exchange therock
    def apimt
      @apimt ||= TheRock.new(
        {
          sl: sql("select sum(btc) btc,sum(eur) eur from #{BD}.mtsl")[0],
          nl: sql("select * from #{BD}.mtlx order by time,txid")
        },
        ops
      )
    end

    # @return [String] comando insert SQL formatado etht (norml)
    def etht_ins
      "insert #{BD}.etht(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,gas,gasprice,gasused,iserror,txreceipt_status,input,contractaddress,dias' \
      ") VALUES#{apies.novtx.map { |obj| etht_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ethi (internas)
    def ethi_ins
      "insert #{BD}.ethi(blocknumber,timestamp,txhash,axfrom,axto,iax," \
      'value,contractaddress,input,type,gas,gasused,traceid,iserror,errcode' \
      ") VALUES#{apies.novix.map { |obj| ethi_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ethp (block)
    def ethp_ins
      "insert #{BD}.ethp(blocknumber,timestamp,blockreward,iax" \
      ") VALUES#{apies.novpx.map { |obj| ethp_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ethw (withdrawals)
    def ethw_ins
      "insert #{BD}.ethw(withdrawalindex,validatorindex,address,amount,blocknumber,timestamp" \
      ") VALUES#{apies.novwx.map { |obj| ethw_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ethk (token)
    def ethk_ins
      "insert #{BD}.ethk(blocknumber,timestamp,txhash,nonce,blockhash,transactionindex,axfrom,axto,iax," \
      'value,tokenname,tokensymbol,tokendecimal,gas,gasprice,gasused,input,contractaddress,dias' \
      ") VALUES#{apies.novkx.map { |obj| ethk_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado eos
    def eost_ins
      "insert #{BD}.eos(gseq,aseq,bnum,time,contract,action,acfrom,acto,iax,amount,moeda,memo,dias" \
      ") VALUES#{apigm.novax.map { |obj| eost_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado det (trades)
    def det_ins
      "insert #{BD}.det(txid,time,tp,user,btc,eur,dtc,dias) VALUES#{apide.trades.map { |obj| det_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado del (ledger)
    def del_ins
      "insert #{BD}.del(txid,time,tp,add,moe,qt,fee) VALUES#{apide.ledger.map { |obj| del_val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado ust (trades)
    def ust_ins
      "insert #{BD}.ust(txid,ordertxid,pair,time,type,ordertype,price,cost,fee,vol,margin,misc,ledgers,dias) " \
      "VALUES#{apius.trades.map { |key, val| ust_1val(key, val) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado usl (ledger)
    def usl_ins
      "insert #{BD}.usl(txid,refid,time,type,aclass,asset,amount,fee) " \
      "VALUES#{apius.ledger.map { |key, val| usl_val(key, val) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado fr (ledger)
    def frl_ins
      "insert #{BD}.fr(uuid,tipo,valor,moe,time,dias) VALUES#{apifr.ledger.map { |obj| frl_val(obj) }.join(',')}"
    end

    # @example (see Beaconchain#formata_saldos)
    # @param (see Beaconchain#formata_saldos)
    # @return [String] valores formatados etht (norml parte1)
    def eth2bh_1val(htb)
      "(#{Integer(htb[:balance])}," \
      "#{Integer(htb[:effectivebalance])}," \
      "#{Integer(htb[:epoch])}," \
      "#{Integer(htb[:validatorindex])})"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao norml etherscan
    # @return [String] valores formatados etht (norml parte1)
    def etht_1val(htx)
      "(#{Integer(htx[:blockNumber])}," \
      "#{Integer(htx[:timeStamp])}," \
      "'#{htx[:hash]}'," \
      "#{Integer(htx[:nonce])}," \
      "'#{htx[:blockHash]}'," \
      "#{Integer(htx[:transactionIndex])}," \
      "'#{htx[:from]}'," \
      "'#{htx[:to]}'," \
      "'#{htx[:iax]}'," \
      "#{etht_2val(htx)}"
    end

    # @param (see etht_1val)
    # @return [String] valores formatados etht (norml parte2)
    def etht_2val(htx)
      txr = htx[:txreceipt_status]
      "cast('#{htx[:value]}' as numeric)," \
      "cast('#{htx[:gas]}' as numeric)," \
      "cast('#{htx[:gasPrice]}' as numeric)," \
      "cast('#{htx[:gasUsed]}' as numeric)," \
      "#{Integer(htx[:isError])}," \
      "#{txr.length.zero? ? 'null' : txr}," \
      "#{etht_3val(htx)}"
    end

    # @param (see etht_1val)
    # @return [String] valores formatados etht (norml parte3)
    def etht_3val(htx)
      cta = htx[:contractAddress]
      inp = htx[:input]
      "#{inp.length.zero? ? 'null' : "'#{inp}'"}," \
      "#{cta.length.zero? ? 'null' : "'#{cta}'"}," \
      "#{Integer(ops[:h][htx[:blockNumber]] || 0)})"
    end

    # @example (see Apibc#inter_es)
    # @param [Hash] htx transacao internas etherscan
    # @return [String] valores formatados ethi (internas parte1)
    def ethi_1val(htx)
      cta = htx[:contractAddress]
      "(#{Integer(htx[:blockNumber])}," \
      "#{Integer(htx[:timeStamp])}," \
      "'#{htx[:hash]}'," \
      "'#{htx[:from]}'," \
      "'#{htx[:to]}'," \
      "'#{htx[:iax]}'," \
      "cast('#{htx[:value]}' as numeric)," \
      "#{cta.length.zero? ? 'null' : "'#{cta}'"}," \
      "#{ethi_2val(htx)}"
    end

    # @param (see ethi_1val)
    # @return [String] valores formatados ethi (internas parte2)
    def ethi_2val(htx)
      inp = htx[:input]
      tid = htx[:traceId]
      txr = htx[:errCode]
      "#{inp.length.zero? ? 'null' : "'#{inp}'"}," \
      "'#{htx[:type]}'," \
      "cast('#{htx[:gas]}' as numeric)," \
      "cast('#{htx[:gasUsed]}' as numeric)," \
      "#{tid.length.zero? ? 'null' : "'#{tid}'"}," \
      "#{Integer(htx[:isError])}," \
      "#{txr.length.zero? ? 'null' : txr})"
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao block etherscan
    # @return [String] valores formatados ethi (block parte1)
    def ethp_1val(htx)
      "(#{Integer(htx[:blockNumber])}," \
      "#{Integer(htx[:timeStamp])}," \
      "cast('#{htx[:blockReward]}' as numeric)," \
      "'#{htx[:iax]}')"
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] valores formatados ethi (withdrawals parte1)
    def ethw_1val(htx)
      "(#{Integer(htx[:withdrawalIndex])}," \
      "#{Integer(htx[:validatorIndex])}," \
      "'#{htx[:address]}'," \
      "cast('#{htx[:amount]}' as numeric)," \
      "#{Integer(htx[:blockNumber])}," \
      "#{Integer(htx[:timestamp])})"
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx token event etherscan
    # @return [String] valores formatados ethk (token parte1)
    def ethk_1val(hkx)
      "(#{Integer(hkx[:blockNumber])}," \
      "#{Integer(hkx[:timeStamp])}," \
      "'#{hkx[:hash]}'," \
      "#{Integer(hkx[:nonce])}," \
      "'#{hkx[:blockHash]}'," \
      "#{Integer(hkx[:transactionIndex])}," \
      "'#{hkx[:from]}'," \
      "'#{hkx[:to]}'," \
      "'#{hkx[:iax]}'," \
      "#{ethk_2val(hkx)}"
    end

    # @param (see ethk_1val)
    # @return [String] valores formatados ethk (token parte2)
    def ethk_2val(hkx)
      "cast('#{hkx[:value]}' as numeric)," \
      "'#{hkx[:tokenName]}'," \
      "'#{hkx[:tokenSymbol]}'," \
      "#{Integer(hkx[:tokenDecimal])}," \
      "cast('#{hkx[:gas]}' as numeric)," \
      "cast('#{hkx[:gasPrice]}' as numeric)," \
      "cast('#{hkx[:gasUsed]}' as numeric)," \
      "#{ethk_3val(hkx)}"
    end

    # @param (see ethk_1val)
    # @return [String] valores formatados ethk (token parte3)
    def ethk_3val(hkx)
      cta = hkx[:contractAddress]
      inp = hkx[:input]
      "#{inp.length.zero? ? 'null' : "'#{inp}'"}," \
      "#{cta.length.zero? ? 'null' : "'#{cta}'"}," \
      "#{Integer(ops[:h][hkx[:blockNumber]] || 0)})"
    end

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] valores formatados para insert eos (parte1)
    def eost_1val(hlx)
      act = hlx[:action_trace][:act]
      "(#{hlx[:global_action_seq]}," \
      "#{hlx[:account_action_seq]}," \
      "#{hlx[:block_num]}," \
      "DATETIME(TIMESTAMP('#{hlx[:block_time]}'))," \
      "'#{act[:account]}'," \
      "'#{act[:name]}'," \
      "#{eost_2val(hlx, act)}"
    end

    # @param (see eost_1val)
    # @param [Hash] act dados da acao
    # @return [String] valores formatados para insert eos (parte2)
    def eost_2val(hlx, act)
      dat = act[:data]
      qtd = dat[:quantity].to_s
      str = dat[:memo].inspect
      "'#{dat[:from]}'," \
      "'#{dat[:to]}'," \
      "'#{hlx[:iax]}'," \
      "#{qtd.to_d},'#{qtd[/[[:upper:]]+/]}'," \
      "nullif('#{str.gsub(/['"]/, '')}','nil')," \
      "#{ops[:h][String(hlx[:itx])] || 0})"
    end

    # @example (see Apice#trades_de)
    # @param [Hash] htx trade bitcoinde
    # @return [String] valores formatados det (trades parte1)
    def det_1val(htx)
      "('#{htx[:trade_id]}'," \
      "DATETIME(TIMESTAMP('#{htx[:successfully_finished_at]}'))," \
      "'#{htx[:type]}'," \
      "'#{htx[:trading_partner_information][:username]}'," \
      "#{det_2val(htx)}"
    end

    # @param (see det_1val)
    # @return [String] valores formatados det (trades parte2)
    def det_2val(htx)
      'cast(' \
      "#{htx[:type] == 'buy' ? htx[:amount_currency_to_trade_after_fee] : "-#{htx[:amount_currency_to_trade]}"}" \
      ' as numeric),' \
      "cast(#{htx[:volume_currency_to_pay_after_fee]} as numeric)," \
      "DATETIME(TIMESTAMP('#{htx[:trade_marked_as_paid_at]}'))," \
      "#{Integer(ops[:h][htx[:trade_id]] || 0)})"
    end

    # @example (see Apice#deposits_de)
    # @example (see Apice#withdrawals_de)
    # @param [Hash] hlx ledger (deposits + withdrawals) bitcoinde
    # @return [String] valores formatados del (ledger)
    def del_val(hlx)
      tip = hlx[:tp]
      "(#{hlx[:txid]}," \
      "DATETIME(TIMESTAMP('#{hlx[:time].iso8601}'))," \
      "'#{tip}'," \
      "'#{hlx[:add]}'," \
      "'#{hlx[:moe]}'," \
      "cast(#{tip == 'withdrawal' ? '-' : ''}#{hlx[:qt]} as numeric)," \
      "cast(#{hlx[:fee]} as numeric))"
    end

    # @example (see Apice#trades_us)
    # @param [String] idx identificador transacao
    # @param [Hash] htx trade kraken
    # @return [String] valores formatados ust (trades parte1)
    def ust_1val(idx, htx)
      "('#{idx}'," \
      "'#{htx[:ordertxid]}'," \
      "'#{htx[:pair]}'," \
      "PARSE_DATETIME('%s', '#{String(htx[:time].round)}')," \
      "'#{htx[:type]}'," \
      "'#{htx[:ordertype]}'," \
      "cast(#{htx[:price]} as numeric)," \
      "cast(#{htx[:cost]} as numeric)," \
      "cast(#{htx[:fee]} as numeric)," \
      "#{ust_2val(idx, htx)}"
    end

    # @param (see ust_1val)
    # @return [String] valores formatados ust (trades parte2)
    def ust_2val(idx, htx)
      msc = htx[:misc].to_s
      "cast(#{htx[:vol]} as numeric)," \
      "cast(#{htx[:margin]} as numeric)," \
      "#{msc.empty? ? 'null' : "'#{msc}'"}," \
      "'#{apius.ledger.select { |_, val| val[:refid] == idx }.keys.join(',') || ''}'," \
      "#{Integer(ops[:h][idx] || 0)})"
    end

    # @example (see Apice#ledger_us)
    # @param idx (see ust_1val)
    # @param [Hash] hlx ledger kraken
    # @return [String] valores formatados usl (ledger)
    def usl_val(idx, hlx)
      acl = hlx[:aclass].to_s
      "('#{idx}'," \
      "'#{hlx[:refid]}'," \
      "PARSE_DATETIME('%s', '#{String(hlx[:time].round)}')," \
      "'#{hlx[:type]}'," \
      "#{acl.empty? ? 'null' : "'#{acl}'"}," \
      "'#{hlx[:asset]}'," \
      "cast(#{hlx[:amount]} as numeric)," \
      "cast(#{hlx[:fee]} as numeric))"
    end

    # @example (see Apice#ledger_fr)
    # @param [Hash] hlx ledger paymium
    # @return [String] valores formatados frl (ledger)
    def frl_val(hlx)
      uid = hlx[:uuid]
      "('#{uid}'," \
      "'#{hlx[:name]}'," \
      "cast(#{hlx[:amount]} as numeric)," \
      "'#{hlx[:currency]}'," \
      "PARSE_DATETIME('%s', '#{hlx[:created_at_int]}')," \
      "#{Integer(ops[:h][uid] || 0)})"
    end

    # @example (see Apice#ledger_mt)
    # @param [Hash] hlx ledger therock
    # @return [String] valores formatados mtl (ledger parte1)
    def mtl_1val(hlx)
      fid = hlx[:fund_id].to_s
      "(#{hlx[:id]}," \
      "DATETIME(TIMESTAMP('#{hlx[:date]}'))," \
      "'#{hlx[:type]}'," \
      "cast(#{hlx[:price]} as numeric)," \
      "'#{hlx[:currency]}'," \
      "#{fid.empty? ? 'null' : "'#{fid}'"}," \
      "#{mtl_2val(hlx)}"
    end

    # @param (see mtl_1val)
    # @return [String] valores formatados mtl (ledger parte2)
    def mtl_2val(hlx)
      nte = hlx[:note].to_s
      tid = hlx[:trade_id].to_s
      "#{nte.empty? ? 'null' : "'#{nte}'"}," \
      "#{tid.empty? ? 'null' : tid.to_s}," \
      "#{Integer(ops[:h][String(hlx[:id])] || 0)})"
    end

    # def eth2at_ins
    #   "insert #{BD}.eth2at(attesterslot,committeeindex,epoch,inclusionslot,status,validatorindex" \
    #   ") VALUES#{apibc.novtx.map { |obj| eth2at_1val(obj) }.join(',')}"
    # end
    # def eth2pr_ins
    #   "insert #{BD}.eth2pr(attestationscount,attesterslashingscount,blockroot,depositscount,epoch," \
    #     'eth1data_blockhash,eth1data_depositcount,eth1data_depositroot,graffiti,graffiti_text,parentroot,' \
    #     'proposer,proposerslashingscount,randaoreveal,signature,slot,stateroot,status,voluntaryexitscount' \
    #   ") VALUES#{apibc.novkx.map { |obj| eth2pr_1val(obj) }.join(',')}"
    # end
    # def eth2at_1val(htx)
    #   "(#{Integer(htx[:attesterslot])}," \
    #   "#{Integer(htx[:committeeindex])}," \
    #   "#{Integer(htx[:epoch])}," \
    #   "#{Integer(htx[:inclusionslot])}," \
    #   "#{Integer(htx[:status])}," \
    #   "#{Integer(htx[:validatorindex])})"
    # end
    # def eth2pr_1val(htx)
    #   "(#{Integer(htx[:attestationscount])}," \
    #   "#{Integer(htx[:attesterslashingscount])}," \
    #   "'#{htx[:blockroot]}'," \
    #   "#{Integer(htx[:depositscount])}," \
    #   "#{Integer(htx[:epoch])}," \
    #   "'#{htx[:eth1data_blockhash]}'," \
    #   "#{eth2pr_2val(htx)}"
    # end
    # def eth2pr_2val(htx)
    #   grf = htx[:graffiti_text]
    #   "#{Integer(htx[:eth1data_depositcount])}," \
    #   "'#{htx[:eth1data_depositroot]}'," \
    #   "'#{htx[:graffiti]}'," \
    #   "#{grf.length.zero? ? 'null' : "'#{grf}'"}," \
    #   "'#{htx[:parentroot]}'," \
    #   "#{Integer(htx[:proposer])}," \
    #   "#{eth2pr_3val(htx)}"
    # end
    # def eth2pr_3val(htx)
    #   "#{Integer(htx[:proposerslashingscount])}," \
    #   "'#{htx[:randaoreveal]}'," \
    #   "'#{htx[:signature]}'," \
    #   "#{Integer(htx[:slot])}," \
    #   "'#{htx[:stateroot]}'," \
    #   "#{Integer(htx[:status])}," \
    #   "#{Integer(htx[:voluntaryexitscount])})"
    # end
  end
end
