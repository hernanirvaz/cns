# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # chaves a eliminar da API - resultado deve ser ignirado pois muda a cada pedido API feito
  DL = %i[cumulativeGasUsed confirmations].freeze

  # classe para processar transacoes do etherscan
  class Etherscan
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes normais & tokens?
    # @return [Etherscan] API etherscan - processar transacoes normais e tokens
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # @return [Array<Hash>] lista transacoes normais novas
    def novnetht
      @novnetht ||= bcd.map { |obc| obc[:tx].select { |obj| idt.include?(obj[:itx]) } }.flatten.uniq { |itm| itm[:itx] }
    end

    # @return [Array<Hash>] lista transacoes internas novas
    def novnethi
      @novnethi ||= bcd.map { |obc| obc[:ix].select { |obj| idi.include?(obj[:itx]) } }.flatten.uniq { |itm| itm[:itx] }
    end

    # @return [Array<Hash>] lista transacoes block novas
    def novnethp
      @novnethp ||= bcd.map { |obc| obc[:px].select { |obj| idp.include?(obj[:itx]) } }.flatten.uniq { |itm| itm[:itx] }
    end

    # @return [Array<Hash>] lista transacoes withdrawals novas
    def novnethw
      @novnethw ||= bcd.map { |obc| obc[:wx].select { |obj| idw.include?(obj[:itx]) } }.flatten.uniq { |itm| itm[:itx] }
    end

    # @return [Array<Hash>] lista transacoes token novas
    def novnethk
      @novnethk ||= bcd.map { |obc| obc[:kx].select { |obj| idk.include?(obj[:itx]) } }.flatten.uniq { |itm| itm[:itx] }
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= bqd[:wb].map { |obj| obj[:ax] }
    end

    # @return [Array<Hash>] todos os dados etherscan - saldos & transacoes
    def bcd
      @bcd ||= api.account_es(lax).map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & etherscan
    def dados
      @dados ||= bqd[:wb].map { |obq| bq_bc(obq, bcd.select { |obc| obq[:ax] == obc[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes normais novas
    def idt
      @idt ||= bcd.map { |obc| obc[:tx].map { |obj| obj[:itx] } }.flatten - (ops[:t] ? [] : bqd[:nt].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes internas novas
    def idi
      @idi ||= bcd.map { |obc| obc[:ix].map { |obj| obj[:itx] } }.flatten - (ops[:t] ? [] : bqd[:ni].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes block novas
    def idp
      @idp ||= bcd.map { |obc| obc[:px].map { |obj| obj[:itx] } }.flatten - (ops[:t] ? [] : bqd[:np].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes withdrawals novas
    def idw
      @idw ||= bcd.map { |obc| obc[:wx].map { |obj| obj[:itx] } }.flatten - (ops[:t] ? [] : bqd[:nw].map { |obq| obq[:itx] })
    end

    # @return [Array<Integer>] lista indices transacoes token novas
    def idk
      @idk ||= bcd.map { |obc| obc[:kx].map { |obj| obj[:itx] } }.flatten - (ops[:t] ? [] : bqd[:nk].map { |obq| obq[:itx] })
    end

    # @example (see Apibc#account_es)
    # @param [Hash] abc account etherscan
    # @return [Hash] dados etherscan - address, saldo & transacoes
    def base_bc(abc)
      acc = abc[:account].downcase
      {
        ax: acc,
        sl: abc[:balance].to_d / (10**18),
        tx: ftik(acc, api.norml_es(acc)),
        ix: ftik(acc, api.inter_es(acc)),
        px: fppp(acc, api.block_es(acc)),
        wx: fwww(acc, api.withw_es(acc)),
        kx: ftik(acc, api.token_es(acc))
      }
    end

    # @param [Hash] wbq wallet bigquery
    # @param [Hash] hbc dados etherscan - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & etherscan
    def bq_bc(wbq, hbc)
      {
        id: wbq[:id],
        ax: xbq = wbq[:ax],
        bs: wbq[:sl],
        bt: bqd[:nt].select { |ont| ont[:iax].casecmp?(xbq) },
        bi: bqd[:ni].select { |oni| oni[:iax].casecmp?(xbq) },
        bp: bqd[:np].select { |onp| onp[:iax].casecmp?(xbq) },
        bw: bqd[:nw].select { |onw| onw[:iax].casecmp?(xbq) },
        bk: bqd[:nk].select { |onk| onk[:iax].casecmp?(xbq) },
        es: hbc[:sl],
        et: hbc[:tx],
        ei: hbc[:ix],
        ep: hbc[:px],
        ew: hbc[:wx],
        ek: hbc[:kx]
      }
    end

    # @return [Array<Hash>] lista ordenada transacoes normais novas
    def sortx
      novnetht.sort_by { |itm| -itm[:srx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes internas novas
    def sorix
      novnethi.sort_by { |itm| -itm[:srx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes block novas
    def sorpx
      novnethp.sort_by { |itm| -itm[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes withdrawals novas
    def sorwx
      novnethw.sort_by { |itm| -itm[:itx] }
    end

    # @return [Array<Hash>] lista ordenada transacoes token novas
    def sorkx
      novnethk.sort_by { |itm| -itm[:srx] }
    end

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mresumo_simples
      return unless dados.count.positive?

      puts("\nid     address                                        etherscan      bigquery")
      dados.each { |obj| puts(formata_carteira_simples(obj)) }
      mtx_norml
      mtx_inter
      mtx_block
      mtx_token
      mtx_withw
      mconfiguracao_ajuste_dias
    end

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mresumo
      return unless dados.count.positive?

      puts("\nid     address      etherscan  tn ti tb tk   tw    bigquery  tn ti tb tk   tw")
      dados.each { |obj| puts(formata_carteira(obj)) }
      mtx_norml
      mtx_inter
      mtx_block
      mtx_token
      mtx_withw
      mconfiguracao_ajuste_dias
    end

    # @param [Hash] hjn dados juntos bigquery & etherscan
    # @return [String] texto formatado duma carteira
    def formata_carteira_simples(hjn)
      format('%<s1>-6.6s %<s2>-42.42s ', s1: hjn[:id], s2: hjn[:ax]) + formata_valores_simples(hjn)
    end

    # @param [Hash] hjn dados juntos bigquery & etherscan
    # @return [String] texto formatado duma carteira
    def formata_carteira(hjn)
      format('%<s1>-6.6s %<s2>-10.10s ', s1: hjn[:id], s2: formata_enderec1(hjn[:ax], 10)) + formata_valores(hjn)
    end

    # @param (see formata_carteira)
    # @return [String] texto formatado valores duma carteira
    def formata_valores_simples(hjn)
      format('%<v1>13.6f %<v2>13.6f %<ok>-3s', v1: hjn[:es], v2: hjn[:bs], ok: ok?(hjn) ? 'OK' : 'NOK')
    end

    # @param (see formata_carteira)
    # @return [String] texto formatado valores duma carteira
    def formata_valores(hjn)
      format(
        '%<v1>11.4f %<n1>3i %<n2>2i %<n3>2i %<n4>2i %<w1>4i %<v2>11.4f %<n5>3i %<n6>2i %<n7>2i %<n8>2i %<w2>4i %<ok>-3s',
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

    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:es].round(6) == hjn[:bs].round(6) && hjn[:bi].count == hjn[:ei].count && hjn[:bp].count == hjn[:ep].count && hjn[:bw].count == hjn[:ew].count
    end

    # @example ether address inicio..fim
    #  0x10f3a0cf0b534c..c033cf32e8a03586
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def formata_enderec1(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = Integer(max / 2) + 4
      inf = max % 2
      "#{add[0, ini - 3]}..#{add[-inf - ini + 5..]}"
    end

    # @example ether address inicio..fim
    #  me-app..4b437776403d
    # @param [Integer] max chars a mostrar
    # @return [String] endereco formatado
    def formata_enderec2(add, max)
      return 'erro' if max < 7

      max -= 2
      ini = Integer(max / 2)
      inf = max % 2
      hid = bqd[:wb].select { |obj| obj[:ax] == add }.first
      ndd = hid ? "#{hid[:id]}-#{add}" : add
      "#{ndd[0, ini]}..#{ndd[-inf - ini..]}"
    end

    # @example (see Apibc#norml_es)
    # @param [Hash] htx transacao normal etherscan
    # @return [String] texto formatado transacao normal etherscan
    def formata_tx_ti(htx)
      format(
        '%<hx>-29.29s %<fr>-15.15s %<to>-15.15s %<dt>10.10s %<vl>7.3f',
        hx: formata_enderec1(htx[:hash], 29),
        fr: formata_enderec2(htx[:from], 15),
        to: formata_enderec2(htx[:to], 15),
        dt: htx[:timeStamp].strftime('%F'),
        vl: htx[:value] / (10**18)
      )
    end

    # @example (see Apibc#token_es)
    # @param [Hash] hkx transacao token etherscan
    # @return [String] texto formatado transacao token etherscan
    def formata_tx_token(hkx)
      format(
        '%<hx>-23.23s %<fr>-15.15s %<to>-15.15s %<dt>10.10s %<vl>7.3f %<sy>-5.5s',
        hx: formata_enderec1(hkx[:hash], 23),
        fr: formata_enderec2(hkx[:from], 15),
        to: formata_enderec2(hkx[:to], 15),
        dt: hkx[:timeStamp].strftime('%F'),
        vl: hkx[:value] / (10**18),
        sy: hkx[:tokenSymbol]
      )
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao block etherscan
    # @return [String] texto formatado transacao block etherscan
    def formata_tx_block(htx)
      format(
        '%<bn>9i %<fr>-41.41s %<dt>10.10s %<vl>17.6f',
        bn: htx[:blockNumber],
        fr: formata_enderec2(htx[:iax], 41),
        dt: htx[:timeStamp].strftime('%F'),
        vl: htx[:blockReward] / (10**18)
      )
    end

    # @example (see Apibc#block_es)
    # @param [Hash] htx transacao withdrawals etherscan
    # @return [String] texto formatado transacao withdrawals etherscan
    def formata_tx_withw(htx)
      format('%<bn>10i %<vi>9i %<dt>10.10s %<vl>10.6f', bn: htx[:withdrawalIndex], vi: htx[:validatorIndex], dt: htx[:timeStamp].strftime('%F'), vl: htx[:amount] / (10**9))
    end

    # @return [String] texto transacoes normais
    def mtx_norml
      return unless ops[:v] && novnetht.count.positive?

      puts("\ntx normal                     from            to              data         valor")
      sortx.each { |obj| puts(formata_tx_ti(obj)) }
    end

    # @return [String] texto transacoes internas
    def mtx_inter
      return unless ops[:v] && novnethi.count.positive?

      puts("\ntx intern                     from            to              data         valor")
      sorix.each { |obj| puts(formata_tx_ti(obj)) }
    end

    # @return [String] texto transacoes block
    def mtx_block
      return unless ops[:v] && novnethp.count.positive?

      puts("\ntx block  address                                   data                   valor")
      sorpx.each { |obj| puts(formata_tx_block(obj)) }
    end

    # @return [String] texto transacoes token
    def mtx_token
      return unless ops[:v] && novnethk.count.positive?

      puts("\ntx token                from            to              data         valor moeda")
      sorkx.each { |obj| puts(formata_tx_token(obj)) }
    end

    # @return [String] texto transacoes withdrawals
    def mtx_withw
      return unless ops[:v] && novnethw.count.positive?

      puts("\nwithdrawal validator data            valor")
      sorwx.each { |obj| puts(formata_tx_withw(obj)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes (normais & token)
    def mconfiguracao_ajuste_dias
      puts("\najuste dias transacoes normais    \n-h=#{sortx.map { |obj| "#{obj[:hash]}:0"            }.join(' ')}") if novnetht.count.positive?
      puts("\najuste dias transacoes internas   \n-h=#{sorix.map { |obj| "#{obj[:hash]}:0"            }.join(' ')}") if novnethi.count.positive?
      puts("\najuste dias transacoes block      \n-h=#{sorpx.map { |obj| "#{obj[:blockNumber]}:0"     }.join(' ')}") if novnethp.count.positive?
      puts("\najuste dias transacoes token      \n-h=#{sorkx.map { |obj| "#{obj[:hash]}:0"            }.join(' ')}") if novnethk.count.positive?
      puts("\najuste dias transacoes withdrawals\n-h=#{sorwx.map { |obj| "#{obj[:withdrawalIndex]}:0" }.join(' ')}") if novnethw.count.positive?
    end

    private

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista transacoes/token events
    # @return [Array<Hash>] lista transacoes/token events filtrada
    def ftik(add, ary)
      ary.map { |o| o.merge(itx: String(o[:hash]), iax: add, value: o[:value].to_d, srx: (tym = Integer(o[:timeStamp])), timeStamp: Time.at(tym)) }
    end

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista blocks events
    # @return [Array<Hash>] lista blocks events filtrada
    def fppp(add, ary)
      ary.map { |o| o.merge(itx: Integer(o[:blockNumber]), iax: add, blockReward: o[:blockReward].to_d, timeStamp: Time.at(Integer(o[:timeStamp]))) }
    end

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista blocks events
    # @return [Array<Hash>] lista blocks events filtrada
    def fwww(add, ary)
      ary.map { |o| o.merge(itx: Integer(o[:withdrawalIndex]), iax: add, amount: o[:amount].to_d, timeStamp: Time.at(Integer(o[:timestamp]))) }
    end
  end
end
