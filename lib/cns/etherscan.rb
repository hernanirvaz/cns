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

    TT = {
      normal: {
        new: :novnetht,
        format: :formata_tx_ti,
        header: "\ntx normal                     from            to              data         valor",
        sork: :srx,
        adjk: :hash
      },
      internal: {
        new: :novnethi,
        format: :formata_tx_ti,
        header: "\ntx intern                     from            to              data         valor",
        sork: :srx,
        adjk: :hash
      },
      block: {
        new: :novnethp,
        format: :formata_tx_block,
        header: "\ntx block  address                                   data                   valor",
        sork: :itx,
        adjk: :blockNumber
      },
      token: {
        new: :novnethk,
        format: :formata_tx_token,
        header: "\ntx token             from            to              data            valor moeda",
        sork: :srx,
        adjk: :hash
      },
      withdrawal: {
        new: :novnethw,
        format: :formata_tx_withw,
        header: "\nwithdrawal validator data            valor",
        sork: :itx,
        adjk: :withdrawalIndex
      }
    }

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

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mresumo_simples
      return unless dados.any?

      puts("\nid     address                                        etherscan      bigquery")
      dados.each { |obj| puts(formata_carteira_simples(obj)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mresumo
      return unless dados.any?

      puts("\nid     address      etherscan  tn ti tb tk   tw    bigquery  tn ti tb tk   tw")
      dados.each { |obj| puts(formata_carteira(obj)) }
      mtransacoes_novas
      mconfiguracao_ajuste_dias
    end

    private

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
      oks?(hjn) && okipw?(hjn) && hjn[:bt].count == hjn[:et].count && hjn[:bk].count == hjn[:ek].count
    end

    def okipw?(hjn)
      oks?(hjn) && hjn[:bi].count == hjn[:ei].count && hjn[:bp].count == hjn[:ep].count && hjn[:bw].count == hjn[:ew].count
    end

    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def oks?(hjn)
      hjn[:es].round(6) == hjn[:bs].round(6)
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
      hid = bqd[:wb].find { |obj| obj[:ax] == add }
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
        '%<hx>-20.20s %<fr>-15.15s %<to>-15.15s %<dt>10.10s %<vl>10.3f %<sy>-5.5s',
        hx: formata_enderec1(hkx[:hash], 20),
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

    # @return [String] Display all new transactions based on verbose option
    def mtransacoes_novas
      TT.each do |_, cfg|
        ntx = send(cfg[:new])
        next unless ops[:v] && ntx.any?

        puts(cfg[:header])
        ntx.sort_by { |s| -s[cfg[:sork]] }.each { |t| puts(send(cfg[:format], t)) }
      end
    end

    # @return [String] Configuration text for adjusting transaction days
    def mconfiguracao_ajuste_dias
      TT.each do |typ, cfg|
        ntx = send(cfg[:new])
        next unless ntx.any?

        puts("\najuste dias transacoes #{typ}\n-h=#{ntx.sort_by { |s| -s[cfg[:sork]] }.map { |t| "#{t[cfg[:adjk]]}:0" }.join(' ')}")
      end
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
      @dados ||= bqd[:wb].map { |b| bq_bc(b, bcd.find { |e| b[:ax] == e[:ax] }) }
    end

    def show_all?
      ops[:t] || false
    end

    def bqidt
      @bqidt ||= show_all? ? [] : (bqd[:nt]&.map { |i| i[:itx] } || [])
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idt
      @idt ||= bcd.map { |o| o[:tx].map { |i| i[:itx] } }.flatten - bqidt
    end

    def bqidi
      @bqidi ||= show_all? ? [] : (bqd[:ni]&.map { |i| i[:itx] } || [])
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idi
      @idi ||= bcd.map { |o| o[:ix].map { |i| i[:itx] } }.flatten - bqidi
    end

    def bqidp
      @bqidp ||= show_all? ? [] : (bqd[:np]&.map { |i| i[:itx] } || [])
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idp
      @idp ||= bcd.map { |o| o[:px].map { |i| i[:itx] } }.flatten - bqidp
    end

    def bqidw
      @bqidw ||= show_all? ? [] : (bqd[:nw]&.map { |i| i[:itx] } || [])
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idw
      @idw ||= bcd.map { |o| o[:wx].map { |i| i[:itx] } }.flatten - bqidw
    end

    def bqidk
      @bqidk ||= show_all? ? [] : (bqd[:nk]&.map { |i| i[:itx] } || [])
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idk
      @idk ||= bcd.map { |o| o[:kx].map { |i| i[:itx] } }.flatten - bqidk
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
      xbq = wbq[:ax]
      {
        id: wbq[:id],
        ax: xbq,
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

    def pess(itm)
      tym = Integer(itm[:timeStamp])
      itm.merge(srx: tym, timeStamp: Time.at(tym))
    end

    # @param add (see Apibc#norml_es)
    # @param [Array<Hash>] ary lista transacoes/token events
    # @return [Array<Hash>] lista transacoes/token events filtrada
    def ftik(add, ary)
      ary.map { |o| pess(o).merge(itx: String(o[:hash]), iax: add, value: o[:value].to_d) }
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
