# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar bigquery
  class Bigquery
    private

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

    # @return [String] comando insert SQL formatado fr (ledger)
    def mtl_ins
      "insert #{BD}.mt(id,time,type,valor,moe,pair,note,trade_id,dias) " \
      "VALUES#{apimt.ledger.map { |obj| mtl_1val(obj) }.join(',')}"
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
  end
end
