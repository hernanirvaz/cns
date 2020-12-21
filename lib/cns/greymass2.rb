# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar transacoes do greymass
  class Greymass
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
      return unless ops[:v] && novax.count.positive?

      puts("\nsequence num from         to           accao      data              valor moeda")
      sorax.each { |obj| puts(formata_ledger(obj)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes
    def mostra_configuracao_ajuste_dias
      return unless novax.count.positive?

      puts("\nstring ajuste dias\n-h=#{sorax.map { |obj| "#{obj[:itx]}:0" }.join(' ')}")
    end
  end
end
