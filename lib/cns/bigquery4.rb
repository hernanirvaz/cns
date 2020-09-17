# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # (see Bigquery)
  class Bigquery
    # @return [String] comando insert SQL formatado det (trades)
    def det_ins
      "insert #{BD}.det(tp,btc,eur,time,user,id,dtc,dias) VALUES#{apide.trades.map { |h| det_val1(h) }.join(',')}"
    end

    # @example (see Apide#trades)
    # @param [Hash] htx transacao trade apide
    # @return [String] valores formatados det (trades parte1)
    def det_val1(htx)
      "('#{htx[:type]}'," \
      'cast(' \
      "#{htx[:type] == 'buy' ? htx[:amount_currency_to_trade_after_fee] : "-#{htx[:amount_currency_to_trade]}"}" \
      ' as numeric),' \
      "cast(#{htx[:volume_currency_to_pay_after_fee]} as numeric)," \
      "DATETIME(TIMESTAMP('#{htx[:successfully_finished_at]}'))," \
      "'#{htx[:trading_partner_information][:username]}'," \
      "#{det_val2(htx)}"
    end

    # @param (see #det_val1)
    # @return [String] valores formatados det (trades parte2)
    def det_val2(htx)
      "'#{htx[:trade_id]}'," \
        "DATETIME(TIMESTAMP('#{htx[:trade_marked_as_paid_at]}'))," \
      "#{Integer(ops[:h][htx[:trade_id]] || 0)})"
    end

    # @return [String] comando insert SQL formatado ust (trades)
    def ust_ins
      "insert #{BD}.ust(txid,ordertxid,pair,time,type,ordertype,price,cost,fee,vol,margin,misc,ledgers,dias) " \
      "VALUES#{apius.trades.map { |k, v| ust_val1(k, v) }.join(',')}"
    end

    # @example (see Apius#trades)
    # @param [String] idx identificador transacao
    # @param [Hash] htx transacao trade apius
    # @return [String] valores formatados ust (trades parte1)
    def ust_val1(idx, htx)
      "('#{idx}'," \
      "'#{htx[:ordertxid]}'," \
      "'#{htx[:pair]}'," \
      "PARSE_DATETIME('%s', '#{String(htx[:time].round)}')," \
      "'#{htx[:type]}'," \
      "'#{htx[:ordertype]}'," \
      "cast(#{htx[:price]} as numeric)," \
      "cast(#{htx[:cost]} as numeric)," \
      "cast(#{htx[:fee]} as numeric)," \
      "#{ust_val2(idx, htx)}"
    end

    # @param (see #ust_val1)
    # @return [String] valores formatados ust (trades parte2)
    def ust_val2(idx, htx)
      "cast(#{htx[:vol]} as numeric)," \
      "cast(#{htx[:margin]} as numeric)," \
      "#{htx[:misc].to_s.empty? ? 'null' : "'#{htx[:misc]}'"}," \
      "'#{apius.ledger.select { |_, v| v[:refid] == idx }.keys.join(',') || ''}'," \
      "#{Integer(ops[:h][idx] || 0)})"
    end

    # @return [String] comando insert SQL formatado del (ledger)
    def del_ins
      "insert #{BD}.del(time,tp,qtxt,id,qt,fee,lgid) VALUES#{apide.ledger.map { |h| del_val(h) }.join(',')}"
    end

    # @example (see Apide#deposit_hash)
    # @example (see Apide#withdrawal_hash)
    # @param [Hash] hlx transacao uniformizada deposits + withdrawals apide
    # @return [String] valores formatados del (ledger)
    def del_val(hlx)
      "(DATETIME(TIMESTAMP('#{hlx[:time].iso8601}'))," \
      "'#{hlx[:tp]}'," \
      "'#{hlx[:qtxt]}'," \
      "'#{hlx[:id]}'," \
      "cast(#{hlx[:tp] == 'out' ? '-' : ''}#{hlx[:qt]} as numeric)," \
      "cast(#{hlx[:fee]} as numeric)," \
      "#{hlx[:lgid]})"
    end

    # @return [String] comando insert SQL formatado usl (ledger)
    def usl_ins
      "insert #{BD}.usl(txid,refid,time,type,aclass,asset,amount,fee) " \
      "VALUES#{apius.ledger.map { |k, v| usl_val(k, v) }.join(',')}"
    end

    # @example (see Apius#ledger)
    # @param idx (see #ust_val1)
    # @param [Hash] hlx transacao ledger apius
    # @return [String] valores formatados usl (ledger)
    def usl_val(idx, hlx)
      "('#{idx}'," \
      "'#{hlx[:refid]}'," \
      "PARSE_DATETIME('%s', '#{String(hlx[:time].round)}')," \
      "'#{hlx[:type]}'," \
      "#{hlx[:aclass].to_s.empty? ? 'null' : "'#{hlx[:aclass]}'"}," \
      "'#{hlx[:asset]}'," \
      "cast(#{hlx[:amount]} as numeric)," \
      "cast(#{hlx[:fee]} as numeric))"
    end

    # @return [String] comando insert SQL formatado fr (ledger)
    def frl_ins
      "insert #{BD}.fr(uuid,tipo,valor,moe,time,dias) VALUES#{apifr.ledger.map { |h| frl_val(h) }.join(',')}"
    end

    # @example (see Apifr#ledger)
    # @param [Hash] hlx transacao ledger apifr
    # @return [String] valores formatados frl (ledger)
    def frl_val(hlx)
      "('#{hlx[:uuid]}'," \
      "'#{hlx[:name]}'," \
      "cast(#{hlx[:amount]} as numeric)," \
      "'#{hlx[:currency]}'," \
      "PARSE_DATETIME('%s', '#{hlx[:created_at_int]}')," \
      "#{Integer(ops[:h][hlx[:uuid]] || 0)})"
    end

    # @return [String] comando insert SQL formatado fr (ledger)
    def mtl_ins
      "insert #{BD}.mt(id,time,type,valor,moe,pair,note,trade_id,dias) " \
      "VALUES#{apimt.ledger.map { |h| mtl_val1(h) }.join(',')}"
    end

    # @example (see Apimt#ledger)
    # @param [Hash] hlx transacao ledger apimt
    # @return [String] valores formatados mtl (ledger parte1)
    def mtl_val1(hlx)
      "(#{hlx[:id]}," \
      "DATETIME(TIMESTAMP('#{hlx[:date]}'))," \
      "'#{hlx[:type]}'," \
      "cast(#{hlx[:price]} as numeric)," \
      "'#{hlx[:currency]}'," \
      "#{hlx[:fund_id].to_s.empty? ? 'null' : "'#{hlx[:fund_id]}'"}," \
      "#{mtl_val2(hlx)}"
    end

    # @param hlx (see #mtl_val1)
    # @return [String] valores formatados mtl (ledger parte2)
    def mtl_val2(hlx)
      "#{hlx[:note].to_s.empty? ? 'null' : "'#{hlx[:note]}'"}," \
      "#{hlx[:trade_id].to_s.empty? ? 'null' : (hlx[:trade_id]).to_s}," \
      "#{Integer(ops[:h][String(hlx[:id])] || 0)})"
    end
  end
end