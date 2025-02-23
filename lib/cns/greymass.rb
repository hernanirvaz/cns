# frozen_string_literal: true

require('bigdecimal/util')

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do greymass
  class Greymass
    # @return [Apibc] API blockchains
    attr_reader :api
    # @return [Array<Hash>] todos os dados bigquery
    attr_reader :bqd
    # @return [Thor::CoreExt::HashWithIndifferentAccess] opcoes trabalho
    attr_reader :ops

    # @param [Hash] dad todos os dados bigquery
    # @param [Thor::CoreExt::HashWithIndifferentAccess] pop opcoes trabalho
    # @option pop [Hash] :h ({}) configuracao dias ajuste reposicionamento temporal
    # @option pop [Boolean] :v (false) mostra dados transacoes?
    # @option pop [Boolean] :t (false) mostra transacoes todas ou somente novas?
    # @return [Greymass] API greymass - processar transacoes
    def initialize(dad, pop)
      @api = Apibc.new
      @bqd = dad
      @ops = pop.transform_keys(&:to_sym)
    end

    # @return [Array<Hash>] lista transacoes novas
    def novneost
      @novneost ||= bcd.map { |obc| obc[:tx].select { |obj| idt.include?(obj[:itx]) } }.flatten
    end

    # @return [Array<String>] lista dos meus enderecos
    def lax
      @lax ||= bqd[:wb].map { |obj| obj[:ax] }
    end

    # @return [Array<Hash>] todos os dados greymass - saldos & transacoes
    def bcd
      @bcd ||= bqd[:wb].map { |obj| base_bc(obj) }
    end

    # @return [Array<Hash>] todos os dados juntos bigquery & greymass
    def dados
      @dados ||= bqd[:wb].map { |obq| bq_bc(obq, bcd.select { |obj| obq[:ax] == obj[:ax] }.first) }
    end

    # @return [Array<Integer>] lista indices transacoes novas
    def idt
      @idt ||= bcd.map { |obc| obc[:tx].map { |obj| obj[:itx] } }.flatten -
               (ops[:t] ? [] : bqd[:nt].map { |obq| obq[:itx] })
    end

    # @example (see Apibc#account_gm)
    # @param [Hash] wbq wallet bigquery
    # @return [Hash] dados greymass - address, saldo & transacoes
    def base_bc(wbq)
      xbq = wbq[:ax]
      {
        ax: xbq,
        sl: greymass_sl(xbq).reduce(:+),
        tx: filtrar_tx(xbq, api.ledger_gm(xbq))
      }
    end

    # @param wbq (see base_bc)
    # @param [Hash] hbc dados greymass - address, saldo & transacoes
    # @return [Hash] dados juntos bigquery & greymass
    def bq_bc(wbq, hbc)
      xbq = wbq[:ax]
      {
        id: wbq[:id],
        ax: xbq,
        bs: wbq[:sl],
        bt: bqd[:nt].select { |obj| obj[:iax] == xbq },
        es: hbc[:sl],
        et: hbc[:tx]
      }
    end

    # @param (see filtrar_tx)
    # @return [Array<BigDecimal>] lista recursos - liquido, net, spu
    def greymass_sl(add)
      hac = api.account_gm(add)
      htr = hac[:total_resources]
      [hac[:core_liquid_balance].to_d, htr[:net_weight].to_d, htr[:cpu_weight].to_d]
    end

    # @param add (see Apibc#account_gm)
    # @param [Array<Hash>] ary lista transacoes
    # @return [Array<Hash>] lista transacoes filtrada
    def filtrar_tx(add, ary)
      # elimina transferencia from: (lax) to: (add) - esta transferencia aparece em from: (add) to: (lax)
      # adiciona chave indice itx & adiciona identificador da carteira iax
      (ary.delete_if do |odl|
        adt = odl[:action_trace][:act][:data]
        adt[:to] == add && lax.include?(adt[:from])
      end).map { |omp| omp.merge(itx: omp[:global_action_seq], iax: add) }
    end

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mostra_resumo
      return unless dados.count.positive?

      puts("\naddress            greymass  ntx       bigquery  ntx")
      dados.each { |obj| puts(formata_carteira(obj)) }
      mostra_transacoes_novas
      mostra_configuracao_ajuste_dias
    end

    # @param [Hash] hjn dados juntos bigquery & greymass
    # @return [String] texto formatado duma carteira
    def formata_carteira(hjn)
      format(
        '%<s1>-12.12s %<v1>14.4f %<n1>4i %<v2>14.4f %<n2>4i %<ok>-3s',
        s1: hjn[:ax],
        v1: hjn[:es],
        n1: hjn[:et].count,
        v2: hjn[:bs],
        n2: hjn[:bt].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see formata_carteira)
    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs] == hjn[:es] && hjn[:bt].count == hjn[:et].count
    end

    # @example (see Apibc#ledger_gm)
    # @param [Hash] hlx ledger greymass
    # @return [String] texto formatado ledger greymass
    def formata_ledger(hlx)
      format(
        '%<bn>12i %<fr>-12.12s %<to>-12.12s %<ac>-10.10s %<dt>10.10s %<vl>12.4f %<sy>-6.6s',
        ac: (act = hlx[:action_trace][:act])[:name],
        fr: (adt = act[:data])[:from],
        vl: (aqt = adt[:quantity].to_s).to_d,
        bn: hlx[:itx],
        to: adt[:to],
        dt: Date.parse(hlx[:block_time]),
        sy: aqt[/[[:upper:]]+/]
      )
    end

    # @return [String] texto transacoes
    def mostra_transacoes_novas
      return unless ops[:v] && novneost.count.positive?

      puts("\nsequence num from         to           accao      data              valor moeda")
      novneost.sort { |ant, prx| prx[:itx] <=> ant[:itx] }.each { |obj| puts(formata_ledger(obj)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes
    def mostra_configuracao_ajuste_dias
      return unless novneost.count.positive?

      puts("\nstring ajuste dias\n-h=#{novneost.sort { |ant, prx| prx[:itx] <=> ant[:itx] }.map { |obj| "#{obj[:itx]}:0" }.join(' ')}")
    end
  end
end
