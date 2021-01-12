# frozen_string_literal: true

# @author Hernani Rodrigues Vaz
module Cns
  # (see Bigquery)
  class Bigquery
    private

    # @return [String] comando insert SQL formatado fr (ledger)
    def mtl_ins
      "insert #{BD}.mt(id,time,type,valor,moe,pair,note,trade_id,dias) " \
      "VALUES#{apimt.ledger.map { |obj| mtl_1val(obj) }.join(',')}"
    end

    # @return [String] comando insert SQL formatado eth2bh
    def eth2bh_ins
      "insert #{BD}.eth2bh(balance,effectivebalance,epoch,validatorindex" \
        ") VALUES#{apibc.nov[0..1500].map { |obj| eth2bh_1val(obj) }.join(',')}"
    end

    # @return [Etherscan] API blockchain ETH
    def apies
      @apies ||= Etherscan.new(
        {
          wb: sql("select * from #{BD}.walletEth order by 2"),
          nt: sql("select itx,iax from #{BD}.ethtx"),
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
  end
end
