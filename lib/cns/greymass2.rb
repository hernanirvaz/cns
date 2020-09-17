# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # classe para processar carteiras & transacoes
  class Greymass
    # @param [Hash] hjn dados juntos bigquery & greymass
    # @return [String] texto formatado duma carteira
    def formata_carteira(hjn)
      format(
        '%<s1>-12.12s %<v1>14.4f %<n1>4i %<v2>14.4f %<n2>4i %<ok>-3s',
        s1: hjn[:ax],
        v1: hjn[:bs],
        n1: hjn[:bt].count,
        v2: hjn[:es],
        n2: hjn[:et].count,
        ok: ok?(hjn) ? 'OK' : 'NOK'
      )
    end

    # @param (see formata_carteira)
    # @return [Boolean] carteira tem transacoes novas(sim=NOK, nao=OK)?
    def ok?(hjn)
      hjn[:bs] == hjn[:es] && hjn[:bt].count == hjn[:et].count
    end

    # @param [Hash] htx transacao
    # @return [String] texto formatado transacao
    def formata_transacao(htx)
      format(
        '%<bn>12i %<fr>-12.12s %<to>-12.12s %<ac>-10.10s %<dt>10.10s %<vl>12.4f %<sy>-6.6s',
        bn: htx[:itx],
        fr: act_data(htx)[:from],
        to: act_data(htx)[:to],
        ac: act(htx)[:name],
        dt: Date.parse(htx[:block_time]),
        vl: act_data_quantity(htx).to_d,
        sy: act_data_quantity(htx)[/[[:upper:]]+/]
      )
    end

    # @param (see formata_transacao)
    # @return [Hash] dados da acao
    def act(htx)
      htx[:action_trace][:act]
    end

    # @param (see formata_transacao)
    # @return [Hash] dados da acao
    def act_data(htx)
      act(htx)[:data]
    end

    # @param (see formata_transacao)
    # @return [String] dados da quantidade
    def act_data_quantity(htx)
      act_data(htx)[:quantity].to_s
    end

    # @return [String] texto carteiras & transacoes & ajuste dias
    def mostra_resumo
      return unless dados.count.positive?

      puts("\naddress            bigquery  ntx       greymass  ntx")
      dados.each { |e| puts(formata_carteira(e)) }
      mostra_transacoes_novas
      mostra_configuracao_ajuste_dias
    end

    # @return [String] texto transacoes
    def mostra_transacoes_novas
      return unless ops[:v] && novax.count.positive?

      puts("\nsequence num from         to           accao      data              valor moeda")
      sorax.each { |e| puts(formata_transacao(e)) }
    end

    # @return [String] texto configuracao ajuste dias das transacoes
    def mostra_configuracao_ajuste_dias
      return unless novax.count.positive?

      puts("\nstring ajuste dias\n-h=#{sorax.map { |e| "#{e[:itx]}:0" }.join(' ')}")
    end
  end
end
