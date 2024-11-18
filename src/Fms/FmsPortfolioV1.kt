package com.syfe.backend.account.services.portfolio

import com.syfe.backend.account.enums.PortfolioDetailsAPIVersion
import com.syfe.backend.account.enums.TransactionType
import com.syfe.backend.account.models.HistoricalReturnData
import com.syfe.backend.account.services.FmsDataTransformationService
import com.syfe.backend.account.services.FmsService
import com.syfe.backend.geoData.GeoContext
import com.syfe.backend.geoData.portfolio.interfaces.IPortfolioService
import com.syfe.backend.portfolios.services.DepositReminderService
import java.math.BigDecimal
import java.time.LocalDate

class FmsPortfolioV1(
    private val fmsService: FmsService,
    private val fmsDataTransformationService: FmsDataTransformationService,
    private val depositReminderService: DepositReminderService,
    private val portfolioDetailsService: PortfolioDetailsService,
    private val geoContext: GeoContext,
    private val geoPortfolioService: IPortfolioService,
    private val commons: FmsPortfolioCommons,
    portfolioData: PortfolioData
) : FmsPortfolio(
    fmsService,
    fmsDataTransformationService,
    geoContext,
    geoPortfolioService,
    portfolioDetailsService,
    depositReminderService,
    commons,
    PortfolioDetailsAPIVersion.IS_V1,
    portfolioData
) {

    override fun getPortfolio(): MutableMap<String, Any?> {
        val portfolioMap = mutableMapOf<String, Any?>()
        val initialData = fetchInitialPortfolioData(portfolioMap)

        val portfolioTransactions = initialData["portfolioTransactions"] as List<Map<String, Any?>>
        val transactionsWithInCompletedCashFlows = initialData["transactionsWithInCompletedCashFlows"]
        val historicalData = initialData["historicalData"] as List<HistoricalReturnData>
        val historicalNetInvestedAmounts =
            historicalData.map { mapOf("date" to it.date, "value" to it.investmentAmount.toString()) }

        val portfolioSnapshotNav = getPortfolioNav()

        insertBasicPortfolioKeys(portfolioMap, pricingPlan, portfolioTransactions)

        val totalManagementFees = mgmtFees.fold(BigDecimal.ZERO) { acc, it -> acc + it.fee!! }

        @Suppress("UNCHECKED_CAST")
        val currencyConvertedTransactions = portfolioMap["transactions"] as List<Map<String, Any>>
        val cashFlowIn = portfolioDetailsService.getCashFlowIn(portfolioTransactions)
        val dividends = portfolioDetailsService.getDividends(currencyConvertedTransactions)

        portfolioMap.putAll(
            mapOf(
                "portfolioSnapshotNav" to portfolioSnapshotNav.toString(),
                "allTransactions" to transactionsWithInCompletedCashFlows,
                "historicalNavs" to historicalNavs,
                "cashFlowIn" to "0.0000",
                "historicalNetInvestedAmounts" to emptyList<Map<String, String>>(),
                "mgmtFees" to mgmtFees,
                "dividends" to dividends,
                "historicalNetInvestedAmounts" to historicalNetInvestedAmounts,
                "cashFlowIn" to cashFlowIn.toString(),
                "totalMgmtFee" to totalManagementFees.toString(),
            )
        )

        val status = portfolioDetailsService.getPortfolioStatus(portfolio)

        if (status == "dormant") {
            insertDormantPortfolioKeys(portfolioMap, currencyConvertedTransactions, historicalNavs)
            val withdrawalTransaction = currencyConvertedTransactions.firstOrNull {
                it["type"] in arrayOf(
                    TransactionType.TRANSFER_OUT,
                    TransactionType.INTERNAL_TRANSFER_OUT
                )
            }
            if (withdrawalTransaction != null) {
                portfolioMap["historicalNetInvestedAmounts"] = historicalNetInvestedAmounts.filter {
                    LocalDate.parse(it["date"].toString()) < LocalDate.parse(withdrawalTransaction["date"].toString())
                }
            }
        }

        return portfolioMap
    }
}
