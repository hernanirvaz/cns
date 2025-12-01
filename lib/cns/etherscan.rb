# frozen_string_literal: true

require('bigdecimal/util')
require('memoist')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do etherscan
  class Etherscan
    extend Memoist

    # @return [Array<Hash>] todos os dados bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :bqd, :ops

    TT = {
      normal: {
        new: :novxt,
        format: :foti,
        header: "\ntx normal                     from            to              data         valor",
        sork: :srx,
        adjk: :hash
      }.freeze,
      internal: {
        new: :novxi,
        format: :foti,
        header: "\ntx intern                     from            to              data         valor",
        sork: :srx,
        adjk: :hash
      }.freeze,
      block: {
        new: :novxp,
        format: :fop,
        header: "\ntx block  address                                   data                   valor",
        sork: :itx,
        adjk: :blockNumber
      }.freeze,
      token: {
        new: :novxk,
        format: :fok,
        header: "\ntx token             from            to              data            valor moeda",
        sork: :srx,
        adjk: :hash
      }.freeze,
      withdrawal: {
        new: :novxw,
        format: :fow,
        header: "\nwithdrawal validator data            valor",
        sork: :itx,
        adjk: :withdrawalIndex
      }.freeze
    }.freeze

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes
    def initialize(dad, pop)
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # mostra resumo carteiras & transacoes & ajuste dias (com contadores)
    def mresumo
      return if bqexd.none?

      puts("\nid     address      etherscan  tn ti tb tk   tw    bigquery  tn ti tb tk   tw")
      bqexd.each { |o| puts(foct(o)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    private

    # mosta transacoes novas
    def mtransacoes_novas
      TT.each_value do |c|
        next unless ops[:v] && (ntx = send(c[:new])).any?

        puts(c[:header])
        ntx.sort_by { |s| -s[c[:sork]] }.each { |t| puts(send(c[:format], t)) }
      end
    end

    # mostra configuration text for adjusting days
    def mconfiguracao_ajuste_dias
      TT.each do |p, c|
        ntx = send(c[:new])
        next if ntx.none?

        puts("\najuste dias transacoes #{p}\n-h=#{ntx.sort_by { |s| -s[c[:sork]] }.map { |t| "#{t[c[:adjk]]}:0" }.join(' ')}")
      end
    end

    # Format detailed wallet summary with counters
    # @param (see focs)
    # @return [String] texto formatado duma carteira (com contadores)
    def foct(hjn)
      format(
        '%<id>-6.6s %<ax>-10.10s %<es>11.4f %<et>3i %<ei>2i %<ep>2i %<ek>2i %<ew>4i %<bs>11.4f %<bt>3i %<bi>2i %<bp>2i %<bk>2i %<bw>4i %<ok>-3s',
        id: hjn[:id],
        ax: foe1(hjn[:ax], 10),
        es: hjn[:es],
        et: hjn[:et].count,
        ei: hjn[:ei].count,
        ep: hjn[:ep].count,
        ek: hjn[:ek].count,
        ew: hjn[:ew].count,
        bs: hjn[:bs],
        bt: hjn[:bt].count,
        bi: hjn[:bi].count,
        bp: hjn[:bp].count,
        bk: hjn[:bk].count,
        bw: hjn[:bw].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # Check if wallets saldo
    # @param (see focs)
    # @return [Boolean] check saldo
    def ok?(hjn)
      hjn[:es].round(4) == hjn[:bs].round(4)
    end

    # @example ether address inicio..fim
    #  0x10f3a0cf0b534c..c033cf32e8a03586
    # @param [String] add endereco ETH
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def foe1(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = (max / 2).to_i + 4
      inf = max % 2
      "#{add[0, ini - 3]}..#{add[-inf - ini + 5..]}"
    end

    # @example ether address inicio..fim
    #  me-app..4b437776403d
    # @param add (see foe1)
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def foe2(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = (max / 2).to_i
      inf = max % 2
      hid = bqd[:wb].find { |o| o[:ax] == add }
      ndd = hid ? "#{hid[:id]}-#{add}" : add
      "#{ndd[0, ini]}..#{ndd[-inf - ini..]}"
    end

    # Format normal(t)/(i)nternal transaction
    # @param [Hash] htx transacao etherscan
    # @return [String] texto formatado
    def foti(htx)
      format(
        '%<hash>-29.29s %<from>-15.15s %<to>-15.15s %<date>10.10s %<value>7.3f',
        hash: foe1(htx[:hash], 29),
        from: foe2(htx[:from], 15),
        to: foe2(htx[:to], 15),
        date: htx[:timeStamp].strftime('%F'),
        value: htx[:value] / (10**18)
      )
    end

    # Format to(k)en transaction
    # @param [Hash] hkx transacao etherscan
    # @return [String] texto formatado
    def fok(hkx)
      format(
        '%<hash>-20.20s %<from>-15.15s %<to>-15.15s %<date>10.10s %<value>10.3f %<symbol>-5.5s',
        hash: foe1(hkx[:hash], 20),
        from: foe2(hkx[:from], 15),
        to: foe2(hkx[:to], 15),
        date: hkx[:timeStamp].strftime('%F'),
        value: hkx[:value] / (10**18),
        symbol: hkx[:tokenSymbol]
      )
    end

    # Format (p)roduced block transaction
    # @param [Hash] hpx transacao etherscan
    # @return [String] texto formatado
    def fop(hpx)
      format(
        '%<block_number>9i %<address>-41.41s %<date>10.10s %<reward>17.6f',
        block_number: hpx[:blockNumber],
        address: foe2(hpx[:iax], 41),
        date: hpx[:timeStamp].strftime('%F'),
        reward: hpx[:blockReward] / (10**18)
      )
    end

    # Format (w)ithdrawal transaction
    # @param [Hash] hwx transacao etherscan
    # @return [String] texto formatado
    def fow(hwx)
      format(
        '%<index>10i %<validator>9i %<date>10.10s %<amount>10.6f',
        index: hwx[:withdrawalIndex],
        validator: hwx[:validatorIndex],
        date: hwx[:timeStamp].strftime('%F'),
        amount: hwx[:amount] / (10**9)
      )
    end

    # Determine if all transactions should be shown
    # @return [Boolean] mostra todas/novas transacoes
    def show_all?
      ops[:t] || false
    end

    # Numero dias para buscar transacoes
    # @return [Integer] days in the past to get transacoes
    def dias
      ops&.[](:d)&.positive? ? ops[:d] : nil
    end

    # Process timestamp
    # @param [Hash] htx transacao
    # @return [Hash] transaccao filtrada
    def pess(htx)
      tym = htx[:timeStamp].to_i
      htx.merge(srx: tym, timeStamp: Time.at(tym))
    rescue ArgumentError
      htx.merge(srx: 0, timeStamp: Time.at(0))
    end

    # Filter normal(t)/(i)nternal/to(k)en transactions
    # @param add (see foe1)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def ftik(add, ary)
      ary.map { |o| pess(o).merge(itx: o[:hash].to_s, iax: add, value: o[:value].to_d) }
    end

    # Filter (p)roduced blocks transactions
    # @param add (see foe1)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def fppp(add, ary)
      ary.map { |o| o.merge(itx: o[:blockNumber].to_i, iax: add, blockReward: o[:blockReward].to_d, timeStamp: Time.at(o[:timeStamp].to_i)) }
    end

    # Filter (w)ithdrawals transactions
    # @param add (see foe1)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def fwww(add, ary)
      ary.map { |o| o.merge(itx: o[:withdrawalIndex].to_i, iax: add, amount: o[:amount].to_d, timeStamp: Time.at(o[:timestamp].to_i)) }
    end

    # Fetch Etherscan data for an account
    # @param [Hash] aes account etherscan
    # @return [Hash] dados etherscan - address, saldo & transacoes
    def esd(aes)
      acc = aes[:account].downcase
      dys = dias
      {
        ax: acc,
        sl: aes[:balance].to_d / (10**18),
        tx: ftik(acc, api.norml_es(acc, days: dys)),
        ix: ftik(acc, api.inter_es(acc, days: dys)),
        px: fppp(acc, api.block_es(acc)), # block_es (mining) does not support time filtering
        wx: fwww(acc, api.withw_es(acc, days: dys)),
        kx: ftik(acc, api.token_es(acc, days: dys))
      }
    end

    # Combine BigQuery and Etherscan data
    # @param [Hash] wbq wallet bigquery
    # @param [Hash] hes dados etherscan - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & etherscan
    def bqesd(wbq, hes)
      xbq = wbq[:ax]
      {
        id: wbq[:id],
        ax: xbq,
        bs: wbq[:sl],
        bt: bqd[:nt].select { |t| t[:iax].casecmp?(xbq) },
        bi: bqd[:ni].select { |i| i[:iax].casecmp?(xbq) },
        bp: bqd[:np].select { |p| p[:iax].casecmp?(xbq) },
        bw: bqd[:nw].select { |w| w[:iax].casecmp?(xbq) },
        bk: bqd[:nk].select { |k| k[:iax].casecmp?(xbq) },
        es: hes[:sl],
        et: hes[:tx],
        ei: hes[:ix],
        ep: hes[:px],
        ew: hes[:wx],
        ek: hes[:kx]
      }
    end

    # Lazy Etherscan API Initialization
    # @return [Apibc] API instance
    memoize def api
      Apibc.new
    end

    # @return [Array<String>] lista enderecos
    memoize def lax
      bqd[:wb].map { |o| o[:ax] }
    end

    # Fetch all Etherscan data
    # @return [Hash] saldos & transacoes, indexed by address
    memoize def exd
      api.account_es(lax).map { |o| esd(o) }.to_h { |h| [h[:ax], h] }
    end

    # Fetch combined data
    # @return [Array<Hash>] todos os dados juntos bigquery & etherscan
    memoize def bqexd
      bqd[:wb].map { |b| bqesd(b, exd[b[:ax]]) }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    memoize def bqkyt
      show_all? ? [] : bqd[:nt].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    memoize def bqkyi
      show_all? ? [] : bqd[:ni].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    memoize def bqkyp
      show_all? ? [] : bqd[:np].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    memoize def bqkyw
      show_all? ? [] : bqd[:nw].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    memoize def bqkyk
      show_all? ? [] : bqd[:nk].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    memoize def exkyt
      exd.values.map { |o| o[:tx].map { |i| i[:itx] } }.flatten - bqkyt
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    memoize def exkyi
      exd.values.map { |o| o[:ix].map { |i| i[:itx] } }.flatten - bqkyi
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    memoize def exkyp
      exd.values.map { |o| o[:px].map { |i| i[:itx] } }.flatten - bqkyp
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    memoize def exkyw
      exd.values.map { |o| o[:wx].map { |i| i[:itx] } }.flatten - bqkyw
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    memoize def exkyk
      exd.values.map { |o| o[:kx].map { |i| i[:itx] } }.flatten - bqkyk
    end

    # Get new normal transactions
    # @return [Array<Hash>] List of new transactions
    memoize def novxt
      exd.values.map { |o| o[:tx].select { |t| exkyt.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # Get new internal transactions
    # @return [Array<Hash>] List of new transactions
    memoize def novxi
      exd.values.map { |o| o[:ix].select { |t| exkyi.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # Get new produced block transactions
    # @return [Array<Hash>] List of new transactions
    memoize def novxp
      exd.values.map { |o| o[:px].select { |t| exkyp.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # Get new withdrawal transactions
    # @return [Array<Hash>] List of new transactions
    memoize def novxw
      exd.values.map { |o| o[:wx].select { |t| exkyw.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # Get new token transactions
    # @return [Array<Hash>] List of new transactions
    memoize def novxk
      exd.values.map { |o| o[:kx].select { |t| exkyk.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end
  end
end
