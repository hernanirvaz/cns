# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do etherscan
  class Etherscan
    # @return [Apibc] API blockchains
    # @return [Array<Hash>] todos os dados bigquery
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :api, :bqd, :ops

    TT = {
      normal: {
        new: :novnetht,
        format: :foti,
        header: "\ntx normal                     from            to              data         valor",
        sork: :srx,
        adjk: :hash
      },
      internal: {
        new: :novnethi,
        format: :foti,
        header: "\ntx intern                     from            to              data         valor",
        sork: :srx,
        adjk: :hash
      },
      block: {
        new: :novnethp,
        format: :fop,
        header: "\ntx block  address                                   data                   valor",
        sork: :itx,
        adjk: :blockNumber
      },
      token: {
        new: :novnethk,
        format: :fok,
        header: "\ntx token             from            to              data            valor moeda",
        sork: :srx,
        adjk: :hash
      },
      withdrawal: {
        new: :novnethw,
        format: :fow,
        header: "\nwithdrawal validator data            valor",
        sork: :itx,
        adjk: :withdrawalIndex
      }
    }

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes
    # @return [Etherscan] API etherscan - processar transacoes
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # mostra resumo carteiras & transacoes & ajuste dias
    def mresumo_simples
      return unless dados.any?

      puts("\nid     address                                        etherscan      bigquery")
      dados.each { |o| puts(focs(o)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    # mostra resumo carteiras & transacoes & ajuste dias (com contadores)
    def mresumo
      return unless dados.any?

      puts("\nid     address      etherscan  tn ti tb tk   tw    bigquery  tn ti tb tk   tw")
      dados.each { |o| puts(foct(o)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    private

    # mosta transacoes novas
    def mtransacoes_novas
      TT.each do |_, c|
        ntx = send(c[:new])
        next unless ops[:v] && ntx.any?

        puts(c[:header])
        ntx.sort_by { |s| -s[c[:sork]] }.each { |t| puts(send(c[:format], t)) }
      end
    end

    # mostra configuration text for adjusting days
    def mconfiguracao_ajuste_dias
      TT.each do |p, c|
        ntx = send(c[:new])
        next unless ntx.any?

        puts("\najuste dias transacoes #{p}\n-h=#{ntx.sort_by { |s| -s[c[:sork]] }.map { |t| "#{t[c[:adjk]]}:0" }.join(' ')}")
      end
    end

    # @param [Hash] hjn dados juntos bigquery & etherscan
    # @return [String] texto formatado duma carteira
    def focs(hjn)
      format(
        '%<s1>-6.6s %<s2>-42.42s %<v1>13.6f %<v2>13.6f %<ok>-3s',
        s1: hjn[:id],
        s2: hjn[:ax],
        v1: hjn[:es],
        v2: hjn[:bs],
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see focs)
    # @return [String] texto formatado duma carteira (com contadores)
    def foct(hjn)
      format(
        '%<s1>-6.6s %<s2>-10.10s %<v1>11.4f %<n1>3i %<n2>2i %<n3>2i %<n4>2i %<w1>4i %<v2>11.4f %<n5>3i %<n6>2i %<n7>2i %<n8>2i %<w2>4i %<ok>-3s',
        s1: hjn[:id],
        s2: foe1(hjn[:ax], 10),
        v1: hjn[:es],
        n1: hjn[:et].count,
        n2: hjn[:ei].count,
        n3: hjn[:ep].count,
        n4: hjn[:ek].count,
        w1: hjn[:ew].count,
        v2: hjn[:bs],
        n5: hjn[:bt].count,
        n6: hjn[:bi].count,
        n7: hjn[:bp].count,
        n8: hjn[:bk].count,
        w2: hjn[:bw].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see focs)
    # @return [Boolean] check saldo & contadores ipwtk
    def ok?(hjn)
      oks?(hjn) && okipw?(hjn) && hjn[:bt].count == hjn[:et].count && hjn[:bk].count == hjn[:ek].count
    end

    # @param (see focs)
    # @return [Boolean] check contadores ipw
    def okipw?(hjn)
      oks?(hjn) && hjn[:bi].count == hjn[:ei].count && hjn[:bp].count == hjn[:ep].count && hjn[:bw].count == hjn[:ew].count
    end

    # @param (see focs)
    # @return [Boolean] carteira tem transacoes novas (sim=NOK, nao=OK)?
    def oks?(hjn)
      hjn[:es].round(6) == hjn[:bs].round(6)
    end

    # @example ether address inicio..fim
    #  0x10f3a0cf0b534c..c033cf32e8a03586
    # @param [String] add endereco ETH
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def foe1(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = Integer(max / 2) + 4
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
      ini = Integer(max / 2)
      inf = max % 2
      hid = bqd[:wb].find { |o| o[:ax] == add }
      ndd = hid ? "#{hid[:id]}-#{add}" : add
      "#{ndd[0, ini]}..#{ndd[-inf - ini..]}"
    end

    # @param [Hash] htx transacao etherscan normal(t)/(i)nternal
    # @return [String] texto formatado
    def foti(htx)
      format(
        '%<hx>-29.29s %<fr>-15.15s %<to>-15.15s %<dt>10.10s %<vl>7.3f',
        hx: foe1(htx[:hash], 29),
        fr: foe2(htx[:from], 15),
        to: foe2(htx[:to], 15),
        dt: htx[:timeStamp].strftime('%F'),
        vl: htx[:value] / (10**18)
      )
    end

    # @param [Hash] hkx transacao etherscan to(k)en
    # @return [String] texto formatado
    def fok(hkx)
      format(
        '%<hx>-20.20s %<fr>-15.15s %<to>-15.15s %<dt>10.10s %<vl>10.3f %<sy>-5.5s',
        hx: foe1(hkx[:hash], 20),
        fr: foe2(hkx[:from], 15),
        to: foe2(hkx[:to], 15),
        dt: hkx[:timeStamp].strftime('%F'),
        vl: hkx[:value] / (10**18),
        sy: hkx[:tokenSymbol]
      )
    end

    # @param [Hash] hpx transacao etherscan (p)roduced blocks
    # @return [String] texto formatado
    def fop(hpx)
      format('%<bn>9i %<fr>-41.41s %<dt>10.10s %<vl>17.6f', bn: hpx[:blockNumber], fr: foe2(hpx[:iax], 41), dt: hpx[:timeStamp].strftime('%F'), vl: hpx[:blockReward] / (10**18))
    end

    # @param [Hash] hwx transacao etherscan (w)ithdrawals
    # @return [String] texto formatado transacao withdrawals etherscan
    def fow(hwx)
      format('%<bn>10i %<vi>9i %<dt>10.10s %<vl>10.6f', bn: hwx[:withdrawalIndex], vi: hwx[:validatorIndex], dt: hwx[:timeStamp].strftime('%F'), vl: hwx[:amount] / (10**9))
    end

    # @return [Boolean] mostra todas/novas transacoes
    def show_all?
      ops[:t] || false
    end

    # @param [Hash] htx transacao
    # @return [Hash] transaccao filtrada
    def pess(htx)
      tym = Integer(htx[:timeStamp])
      htx.merge(srx: tym, timeStamp: Time.at(tym))
    end

    # @param add (see foe1)
    # @param [Array<Hash>] ary lista transacoes normal(t)/(i)nternal/to(k)en
    # @return [Array<Hash>] lista transacoes filtrada
    def ftik(add, ary)
      ary.map { |o| pess(o).merge(itx: String(o[:hash]), iax: add, value: o[:value].to_d) }
    end

    # @param add (see foe1)
    # @param [Array<Hash>] ary lista transacoes (p)roduced blocks
    # @return [Array<Hash>] lista transacoes filtrada
    def fppp(add, ary)
      ary.map { |o| o.merge(itx: Integer(o[:blockNumber]), iax: add, blockReward: o[:blockReward].to_d, timeStamp: Time.at(Integer(o[:timeStamp]))) }
    end

    # @param add (see foe1)
    # @param [Array<Hash>] ary lista transacoes (w)ithdrawals
    # @return [Array<Hash>] lista transacoes filtrada
    def fwww(add, ary)
      ary.map { |o| o.merge(itx: Integer(o[:withdrawalIndex]), iax: add, amount: o[:amount].to_d, timeStamp: Time.at(Integer(o[:timestamp]))) }
    end

    # @param [Hash] aes account etherscan
    # @return [Hash] dados etherscan - address, saldo & transacoes
    def bses(aes)
      acc = aes[:account].downcase
      {
        ax: acc,
        sl: aes[:balance].to_d / (10**18),
        tx: ftik(acc, api.norml_es(acc)),
        ix: ftik(acc, api.inter_es(acc)),
        px: fppp(acc, api.block_es(acc)),
        wx: fwww(acc, api.withw_es(acc)),
        kx: ftik(acc, api.token_es(acc))
      }
    end

    # @param [Hash] wbq wallet bigquery
    # @param [Hash] hes dados etherscan - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & etherscan
    def bqes(wbq, hes)
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

    # @return [Array<String>] lista enderecos
    def lax
      @lax ||= bqd[:wb].map { |o| o[:ax] }
    end

    # @return [Array<Hash>] todos os dados etherscan - saldos & transacoes
    def esd
      @esd ||= api.account_es(lax).map { |o| bses(o) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & etherscan
    def dados
      @dados ||= bqd[:wb].map { |b| bqes(b, esd.find { |e| b[:ax] == e[:ax] }) }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    def bqidt
      @bqidt ||= show_all? ? [] : bqd[:nt].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    def bqidi
      @bqidi ||= show_all? ? [] : bqd[:ni].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    def bqidp
      @bqidp ||= show_all? ? [] : bqd[:np].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    def bqidw
      @bqidw ||= show_all? ? [] : bqd[:nw].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes bigquery
    def bqidk
      @bqidk ||= show_all? ? [] : bqd[:nk].map { |i| i[:itx] }
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    def idt
      @idt ||= esd.map { |o| o[:tx].map { |i| i[:itx] } }.flatten - bqidt
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    def idi
      @idi ||= esd.map { |o| o[:ix].map { |i| i[:itx] } }.flatten - bqidi
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    def idp
      @idp ||= esd.map { |o| o[:px].map { |i| i[:itx] } }.flatten - bqidp
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    def idw
      @idw ||= esd.map { |o| o[:wx].map { |i| i[:itx] } }.flatten - bqidw
    end

    # @return [Array<Integer>] indices transacoes novas (etherscan - bigquery)
    def idk
      @idk ||= esd.map { |o| o[:kx].map { |i| i[:itx] } }.flatten - bqidk
    end

    # @return [Array<Hash>] lista transacoes normais novas
    def novnetht
      @novnetht ||= esd.map { |o| o[:tx].select { |t| idt.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # @return [Array<Hash>] lista transacoes internas novas
    def novnethi
      @novnethi ||= esd.map { |o| o[:ix].select { |t| idi.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # @return [Array<Hash>] lista transacoes block novas
    def novnethp
      @novnethp ||= esd.map { |o| o[:px].select { |t| idp.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # @return [Array<Hash>] lista transacoes withdrawals novas
    def novnethw
      @novnethw ||= esd.map { |o| o[:wx].select { |t| idw.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end

    # @return [Array<Hash>] lista transacoes token novas
    def novnethk
      @novnethk ||= esd.map { |o| o[:kx].select { |t| idk.include?(t[:itx]) } }.flatten.uniq { |i| i[:itx] }
    end
  end
end
