package com.syfe.backend.account.services.portfolio

import com.syfe.backend.account.enums.CashFlowStates
import com.syfe.backend.account.enums.PortfolioDetailsAPIVersion
import com.syfe.backend.account.enums.ProcessType
import com.syfe.backend.account.enums.TransactionType
import com.syfe.backend.account.models.*
import com.syfe.backend.account.services.FmsDataTransformationService
import com.syfe.backend.account.services.FmsService
import com.syfe.backend.common.enums.Currency
import com.syfe.backend.geoData.GeoContext
import com.syfe.backend.geoData.portfolio.interfaces.IPortfolioService
import com.syfe.backend.portfolios.enums.PortfolioType
import com.syfe.backend.portfolios.models.HistoricalNavDto
import com.syfe.backend.portfolios.models.Portfolio
import com.syfe.backend.portfolios.services.DepositReminderService
import com.syfe.backend.portfolios.services.ProjectedAnnualYield
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate
import java.time.LocalDateTime

data class PortfolioData(
    val userPortfolio: Portfolio,
    val portfolios: List<FMSPortfolioDto>,
    val allUserPortfolios: Map<Long, Portfolio>,
    val mgmtFeeDates: List<String>,
    val depositConfirmed: Boolean,
    val reportingCurrency: Currency,
    val SERVICE_DATE: String,
    val SHOW_INDICATIVE_NAV: Boolean,
    val securities: Map<Int, FMSSecurityDto>,
    val pricingPlan: String,
    val isPartOfTransferPlan: Boolean,
    val projectedAnnualYield: ProjectedAnnualYield? = null
)

abstract class FmsPortfolio(
    private val fmsService: FmsService,
    private val fmsDataTransformationService: FmsDataTransformationService,
    private val geoContext: GeoContext,
    private val geoPortfolioService: IPortfolioService,
    private val portfolioDetailsService: PortfolioDetailsService,
    private val depositReminderService: DepositReminderService,
    private val commons: FmsPortfolioCommons,
    private val apiVersion: PortfolioDetailsAPIVersion,
    portfolioData: PortfolioData
) {

    var security: Map<Int, FMSSecurityDto>
    var fmsHoldings: List<FMSHoldingSnapshotDto>
    var transactions: List<FMSTransactionDTO>
    protected var detailedPortfolioInfo: PortfolioInfoDto?
    private var inProgressWithdrawals: List<FmsWithdrawalDto>
    protected var historicalNavs: List<HistoricalNavDto>
    protected var cashRebates: List<CashRebateDto>
    private var indicativeNav: FMSIndicativeNavResponseDto
    private var withdrawableAmounts: FmsWithdrawableAmounts
    private var cashFlows: List<FMSCashFlowDto>
    private var withdrawals: Map<Int, FmsWithdrawalDto>
    protected var nextBusinessDate: LocalDate
    protected var nextBusinessDateIncludingToday: LocalDate

    val portfolio: FMSPortfolioDto
    val portfolioCurrency: Currency
    var mgmtFees: List<FMSManagementFeeDto> = emptyList()
    var mngnmtFeesAndTotal: ManagementFeeDetailsDtoWithTotal? = null

    protected val logger = KotlinLogging.logger {}

    protected val userPortfolio = portfolioData.userPortfolio
    private val allUserPortfolios = portfolioData.allUserPortfolios
    protected val portfolios = portfolioData.portfolios
    protected val depositConfirmed = portfolioData.depositConfirmed
    protected val isPartOfTransferPlan = portfolioData.isPartOfTransferPlan
    protected val reportingCurrency = portfolioData.reportingCurrency
    private val serviceDateString = portfolioData.SERVICE_DATE
    protected val serviceDate = LocalDate.parse(serviceDateString)
    protected val showIndicativeNav = portfolioData.SHOW_INDICATIVE_NAV
    protected val pricingPlan = portfolioData.pricingPlan
    protected val excludedCashFlowStates = setOf(CashFlowStates.PAYMENT_FAILED)
    protected val projectedAnnualYield = portfolioData.projectedAnnualYield

    protected val INITIAL_MANAGEMENT_FEES_REQ_PAGE = 0
    protected val INITIAL_MANAGEMENT_FEES_REQ_SIZE = 2

    init {
        portfolio = portfolios.find { it.id!! == userPortfolio.id }!!

        runBlocking {
            val securitiesResponse = async {
                fmsService.getSecuritiesAsync()
            }
            val holdingsResponse = async {
                fmsService.getHoldingSnapshotsByDateAsync(portfolio.id!!, LocalDate.parse(serviceDateString))
            }

            val nextBusinessDateResponse = async {
                fmsService.getNextBusinessDateAsync(LocalDate.now(), 1)
            }

            val nextBusinessDateIncludingTodayResponse = async {
                fmsService.getNextBusinessDateAsync(LocalDate.now().minusDays(1), 1)
            }

            val transactionsResponse = async {
                fmsService.getPortfolioTransactionsAsync(portfolio.id!!)
            }

            val portfolioInfoResponse = async {
                fmsService.getPortfolioInfoAsync(portfolio.id!!)
            }

            val inProgressWithdrawalsResponse = async {
                fmsService.getInProgressWithdrawals(portfolio.id!!)
            }

            val historicalNavsResponse = async {
                fmsService.getHistoricalNavsAsync(userPortfolio, reportingCurrency)
            }

            val cashRebatesResponse = async {
                fmsService.getCashRebatesAsync(userPortfolio)
            }

            val indicativeNavResponse = async {
                fmsService.getIndicativeNavResponseAsync(portfolio.id!!, reportingCurrency)
            }

            val withdrawableAmountsResponse = async {
                fmsService.getAllowedWithdrawalAmountsByPortfolioIdAndCurrencyAsync(portfolio.id!!, reportingCurrency)
            }

            val cashFlowsResponse = async {
                fmsService.getCashFlowsByPortfolioIdAndStatesAsync(portfolio.id!!, CashFlowStates.getAll(), ProcessType.CLIENT_API)
            }

            cashFlows = cashFlowsResponse.await()
            val withdrawalIds = cashFlows.filter { it.type in TransactionType.withdrawalTransactionTypes }.mapNotNull { it.withdrawalId }

            val withdrawalsResponse = async {
                fmsService.getWithdrawalsByIdInAsync(withdrawalIds, ProcessType.CLIENT_API).associateBy { it.id!! }
            }

            when (apiVersion) {
                PortfolioDetailsAPIVersion.IS_V1 -> {
                    val managementFeesResponse = async {
                        fmsService.getManagementFeesAsync(userPortfolio!!, reportingCurrency)
                    }
                    mgmtFees = managementFeesResponse.await()
                }

                PortfolioDetailsAPIVersion.IS_V2 -> {
                    val managementFeesResponse = async {
                        fmsService.getPaginatedManagementFeesAsync(userPortfolio.id!!, reportingCurrency, INITIAL_MANAGEMENT_FEES_REQ_SIZE, INITIAL_MANAGEMENT_FEES_REQ_PAGE, true)
                    }
                    mngnmtFeesAndTotal = managementFeesResponse.await()
                }

                PortfolioDetailsAPIVersion.IS_V3 -> {
                    val managementFeesResponse = async {
                        fmsService.getManagementFeesAsync(userPortfolio.id!!, reportingCurrency, INITIAL_MANAGEMENT_FEES_REQ_SIZE, INITIAL_MANAGEMENT_FEES_REQ_PAGE)
                    }
                    mgmtFees = managementFeesResponse.await()
                }
            }

            security = securitiesResponse.await().associateBy { it.id!!.toInt() }
            fmsHoldings = holdingsResponse.await()
            transactions = transactionsResponse.await()
            detailedPortfolioInfo = portfolioInfoResponse.await()
            inProgressWithdrawals = inProgressWithdrawalsResponse.await()
            historicalNavs = fmsDataTransformationService.getNavFromFirstFundedDate(historicalNavsResponse.await().sortedBy { it.date })
            cashRebates = cashRebatesResponse.await()
            indicativeNav = indicativeNavResponse.await()
            withdrawableAmounts = withdrawableAmountsResponse.await()
            withdrawals = withdrawalsResponse.await()
            nextBusinessDate = nextBusinessDateResponse.await()
            nextBusinessDateIncludingToday = nextBusinessDateIncludingTodayResponse.await()
        }

        portfolioCurrency = portfolio.currency!!
    }

    protected fun getBaseComposition(): Map<String, Float> {
        val type = userPortfolio.type

        return PortfolioType.getAssetClassGroup(type).getBaseComposition()
    }

    protected fun getBaseCompositionOrdered(): List<Map<String, Any>> {
        val type = userPortfolio.type

        return PortfolioType.getAssetClassGroup(type).getBaseCompositionOrdered()
    }

    protected fun getPortfolioTransactions(excludedCashFlowStates: Set<CashFlowStates>?): List<Map<String, Any?>> {
        return fmsDataTransformationService.getPortfolioTransactions(
            transactions,
            security,
            serviceDate,
            portfolioCurrency,
            reportingCurrency,
            portfolio.id!!,
            excludedCashFlowStates,
            cashFlows,
            withdrawals,
            allUserPortfolios
        )
    }

    protected fun getPortfolioNav(): BigDecimal {
        return fmsDataTransformationService.getPortfolioNav(portfolioCurrency, reportingCurrency, serviceDate, detailedPortfolioInfo, transactions)
    }

    protected fun getPortfolioDetails(): PortfolioDetails {
        return PortfolioDetails(
            portfolio,
            detailedPortfolioInfo!!,
            reportingCurrency,
            serviceDate,
            inProgressWithdrawals,
            indicativeNav,
            withdrawableAmounts
        )
    }

    protected fun fetchInitialPortfolioData(portfolioMap: MutableMap<String, Any?>,): Map<String, Any> {
        val portfolioType = portfolio.type!!
        val pricingPlan = pricingPlan
        val goalData = portfolioDetailsService.getUserGoalData(userPortfolio, reportingCurrency, serviceDate)

        val portfolioTransactions = getPortfolioTransactions(excludedCashFlowStates)
        val transactionsWithInCompletedCashFlows = fmsDataTransformationService.addInCompletedCashFlows(
            portfolioTransactions,
            reportingCurrency,
            portfolio.id!!
        )

        val inProgressDepositsAndWithdrawals = fmsDataTransformationService.getInProgressDepositsAndWithdrawals(
            transactionsWithInCompletedCashFlows,
            reportingCurrency,
            portfolio.id!!
        )
        val completedTransactionsWithoutQuickWithdrawals = portfolioTransactions.filterNot { (it["status"] == "Processing" && it["type"] in TransactionType.withdrawalOutTransactionTypes) || (it["isQuickWithdrawalWithStatusProcessingTransfer"] == true) }
        val transactionsWithInCompletedDepositsAndWithdrawals = inProgressDepositsAndWithdrawals.plus(completedTransactionsWithoutQuickWithdrawals)
            .sortedWith(
                compareBy(
                    { it["date"] as LocalDate },
                    { it["createdAt"] as LocalDateTime }
                )
            ).reversed()

        val groupedTransactions = fmsDataTransformationService.groupAllTransactions(transactionsWithInCompletedDepositsAndWithdrawals, portfolioType)

        val transactionsPreComputedData = fmsService.getTransactionsPrecomputedData(portfolio.id!!, reportingCurrency)

        portfolioMap.putAll(
            mapOf(
                "goalPeriod" to userPortfolio.goalPeriod,
                "goalAmount" to userPortfolio.goalValue,
                "depositOneTime" to userPortfolio.depositOneTime.setScale(2, RoundingMode.HALF_EVEN).toString(),
                "depositMonthly" to userPortfolio.depositMonthly.setScale(2, RoundingMode.HALF_EVEN).toString(),
                "baseCurrency" to portfolioCurrency,
                "principal" to "0.000000",
                "indicativeTicksUpdateDate" to "",
                "pnlInceptionPortfolio" to "0.000000",
                "totalDividend" to "0.000000",
                "dividendPayout" to mapOf(
                    "nextQuarter" to mapOf(
                        "amount" to "0.000000",
                        "date" to null
                    ),
                    "total" to "0.000000",
                    "all" to emptyArray<Map<String, Any?>>()
                ),
                "composition" to getBaseComposition(), // deprecated. use compositionOrdered instead.
                "compositionOrdered" to getBaseCompositionOrdered(),
                "rebalancingHistory" to emptyList<String>(), // needed for app
                "rebalanceHistory" to emptyList<Map<String, String>>(),
                "varCurrent" to portfolio.riskActual,
                "inceptionDate" to transactions.filter { it.type in arrayOf(TransactionType.TRANSFER_IN, TransactionType.INTERNAL_TRANSFER_IN) }.minByOrNull { it.tradeDate!! }?.tradeDate,
                "holdings" to emptyArray<Map<String, Any?>>(),
                "groupedTransactions" to groupedTransactions,
                "depositReminder" to depositReminderService.getDepositReminder(userPortfolio.id!!),
                "goalData" to goalData,
                "fundSource" to portfolioType.config.fundSource,
                "additionalDataForFundSource" to commons.getPortfolioDataForFundSource(
                    portfolioType.config.fundSource,
                    portfolio.srsFundingStatus,
                    portfolio.fundingRealisationDate,
                ),
                "mgmtFees" to emptyArray<Map<String, Any?>>(),
            )
        )

        val portfolioDetails = getPortfolioDetails()

        @Suppress("UNCHECKED_CAST")
        val currencyConvertedTransactions = portfolioTransactions as List<Map<String, Any>>

        val cashFlowIn = transactionsPreComputedData.cashflowIn
        val cashFlowOut = transactionsPreComputedData.cashFlowOut
        val dividendPayoutCashFlow = transactionsPreComputedData.cashFlowDividendPayout

        val indicativeNavResponse = portfolioDetailsService.getIndicativeNav(portfolioDetails, transactions)
        val pnlInceptionPortfolio = portfolioDetailsService.getPnlInceptionPortfolio(portfolioDetails, indicativeNavResponse, showIndicativeNav)

        val nav = portfolioDetailsService.getNav(portfolioDetails, indicativeNavResponse, transactions, showIndicativeNav)

        val twrValue = portfolioDetailsService.getTwrValue(indicativeNavResponse, showIndicativeNav, detailedPortfolioInfo!!, reportingCurrency)
        val lastUpdateTimestamp = portfolioDetailsService.getLastUpdatedTimestamp(indicativeNavResponse, showIndicativeNav)
        val lastUpdateMessage = portfolioDetailsService.getLastUpdatedMessage(indicativeNavResponse, showIndicativeNav)
        val indicativeTicksUpdateDate = portfolioDetailsService.getIndicativeTicksUpdateDate(indicativeNavResponse, showIndicativeNav)

        val status = portfolioDetailsService.getPortfolioStatus(portfolio)
        val twrPercent = portfolioDetailsService.formatTwrPercent(twrValue)

        val holdings = portfolioDetailsService.getHoldings(fmsHoldings, portfolioDetails, security, nav, userPortfolio.type, userPortfolio.theme)

        val historicalData =
            fmsDataTransformationService.getHistoricalReturnData(historicalNavs, portfolioTransactions, userPortfolio.theme)

        var lifetimeValue = nav

        if (userPortfolio.type in geoPortfolioService.getPortfolioTypesWithDividends()) {
            val dividendPayout = portfolioDetailsService.getDividendPayout(
                currencyConvertedTransactions,
                portfolio,
                reportingCurrency,
                serviceDate
            )
            portfolioMap["dividendPayout"] = dividendPayout

            lifetimeValue += dividendPayout["total"] as java.math.BigDecimal
        }

        val netInvestedAmount = cashFlowIn - cashFlowOut + dividendPayoutCashFlow
        val pnlInception = portfolioDetailsService.getPnlInception(nav, cashFlowIn, cashFlowOut)
        val pnlInceptionPercentage = portfolioDetailsService.getPnlInceptionPercentage(netInvestedAmount, pnlInception, userPortfolio.userId!!)

        val portfolioReturnOptionDto =
            fmsDataTransformationService.getPortfolioReturnOptionDetails(historicalData, userPortfolio)

        val investorId = portfolioDetails.portfolio.investorId!!

        val customRebalancingStatus =
            portfolioDetailsService.getCustomRebalancingStatus(
                userPortfolio,
                portfolio,
                portfolioDetails.inProgressWithdrawals
            )

        portfolioMap["customRebalancing"] = mapOf(
            "status" to customRebalancingStatus.status,
            "enabled" to customRebalancingStatus.isEnabled
        )

        portfolioMap["eligibilityForCashPayout"] = geoPortfolioService.isEligibleForPayout(
            portfolioType,
            mapOf(
                "nav" to nav,
                "netInvestedAmount" to netInvestedAmount,
                "tier" to pricingPlan
            )
        )

        logger.info {
            "Portfolio Id: ${portfolio.id} " +
                    "IndicativeNav: ${indicativeNavResponse.indicativeNav}, " +
                    "CurrentNav: $nav, " +
                    "IndicativeTicksUpdateDate: ${indicativeNavResponse.indicativeTicksUpdateDate}, " +
                    "IndicativeNavUpdateTime: ${indicativeNavResponse.indicativeNavUpdateTime}, " +
                    "netInvestedAmount: $netInvestedAmount, " +
                    "pnlInception: $pnlInception, " +
                    "lifetimeValue $lifetimeValue"
        }

        portfolioMap.putAll(
            mapOf(
                "status" to status,
                "principal" to cashFlowIn.toString(),
                "netInvestedAmount" to netInvestedAmount.toString(),
                "nav" to nav.toString(),
                "indicativeTicksUpdateDate" to indicativeTicksUpdateDate,
                "pnlInception" to pnlInception.toString(),
                "pnlInceptionPercent" to pnlInceptionPercentage,
                "pnlInceptionPortfolio" to pnlInceptionPortfolio.toString(),
                "twrPercent" to twrPercent.toString(),
                "totalDividend" to transactionsPreComputedData.totalDividend.toString(),
                "lifetimeValue" to lifetimeValue.toString(),
                "holdings" to holdings,
                "lastUpdateTimestamp" to lastUpdateTimestamp,
                "lastUpdateMessage" to lastUpdateMessage,
                "depositConfirmed" to if (portfolio.status == "INACTIVE") depositConfirmed else true,
                "guaranteedPortfolioDetails" to portfolioDetailsService.getGuaranteedPortfolioDetails(
                    portfolio = portfolio,
                    userPortfolio = userPortfolio,
                    currentValue = indicativeNavResponse.indicativeNav,
                    maturityOptionInfoDto = userPortfolio.maturityOptionInfo,
                    nextBusinessDate,
                    nextBusinessDateIncludingToday,
                    if (portfolio.status == "INACTIVE") depositConfirmed else true,
                    isPartOfTransferPlan
                ),
                "isPartOfTransferPlan" to isPartOfTransferPlan,
                "projectedAnnualIncome" to projectedAnnualYield?.yield?.let {
                    (netInvestedAmount * it).divide(
                        BigDecimal("100")
                    )
                },
                "projectedAnnualYield" to projectedAnnualYield?.projectedAnnualYield,
                "projectedAnnualYieldParity" to projectedAnnualYield?.projectedAnnualYieldParity,
                "historicalData" to historicalData,
                "portfolioReturnOption" to portfolioReturnOptionDto.portfolioReturnOption,
                "isReturnOptionToggleEnabled" to portfolioReturnOptionDto.isReturnOptionToggleEnabled,
            )
        )

        val aggregatedHoldings = portfolioDetailsService.getAggregatedHoldings(holdings)

        @Suppress("UNCHECKED_CAST")
        portfolioMap["composition"] = (portfolioMap["composition"] as Map<String, Any?>) + aggregatedHoldings

        @Suppress("UNCHECKED_CAST")
        portfolioMap["compositionOrdered"] = (portfolioMap["compositionOrdered"] as List<Map<String, Any>>).map {
            mapOf(
                "category" to it["category"],
                "weightage" to (aggregatedHoldings[it["category"]] ?: it["weightage"])
            )
        }

        if (status == "dormant") {
            insertDormantPortfolioKeys(portfolioMap, currencyConvertedTransactions, historicalNavs)
        }

        if (userPortfolio.type in geoPortfolioService.getPortfolioTypesWithRebates()) {
            portfolioMap["totalCashRebate"] = cashRebates.map { it.cashRebatePaid }.fold(BigDecimal.ZERO) { acc, it -> acc + it }
            portfolioMap["cashRebates"] = cashRebates
        }

        insertWithdrawableAmounts(portfolioMap, investorId, portfolioDetails)

        return mapOf(
            "groupedTransactions" to groupedTransactions,
            "transactionsPreComputedData" to transactionsPreComputedData,
            "portfolioTransactions" to portfolioTransactions,
            "transactionsWithInCompletedCashFlows" to transactionsWithInCompletedCashFlows,
            "historicalData" to historicalData
        )
    }

    protected fun insertBasicPortfolioKeys(
        portfolioMap: MutableMap<String, Any?>,
        pricingPlan: String,
        portfolioTransactions: List<Map<String, Any?>>
    ) {
        val activeCurrencies = geoContext.currencyService.getActiveCurrencies()
        val portfolioType = geoPortfolioService.getGroupedPortfolioTypes(userPortfolio.type)
        portfolioMap.putAll(
            mapOf(
                "currency" to reportingCurrency,
                "dividendOption" to portfolio.dividendOption,
                "goalObjective" to userPortfolio.goalObjective,
                "id" to portfolio.id.toString(),
                "name" to userPortfolio.name,
                "nav" to "0.000000",
                "netInvestedAmount" to "0.000000",
                "pnlInception" to "0.000000",
                "pnlInceptionPercent" to null,
                "status" to "inactive",
                "twrPercent" to "0.000000",
                "type" to userPortfolio.type,
                "portfolioType" to portfolioType?.displayName,
                "theme" to userPortfolio.theme,
                "etfs" to userPortfolio.etfs,
                "varChosen" to portfolio.riskTarget,
                "createdOn" to portfolio.createdAt!!.toLocalDate().toString(),
                "createdAt" to portfolio.createdAt!!,
                "transactions" to portfolioTransactions,
                "pricingPlan" to pricingPlan,
                "lastUpdateTimestamp" to portfolio.createdAt,
                "depositConfirmed" to if (portfolio.status == "INACTIVE") depositConfirmed else true,
                "referenceCode" to userPortfolio.referenceCode,
                "withdrawableAmounts" to activeCurrencies.associateWith { BigDecimal.ZERO },
                "quickWithdrawableAmounts" to activeCurrencies.associateWith { BigDecimal.ZERO },
                "isWithdrawalBlocked" to userPortfolio.isWithdrawalBlocked,
                "portfolioReturnOption" to userPortfolio.portfolioReturnOption,
                "projectedAnnualIncome" to null,
                "projectedAnnualYield" to null,
                "historicalData" to null,
                "template" to userPortfolio.template,
            )
        )
    }

    protected fun insertDormantPortfolioKeys(portfolioMap: MutableMap<String, Any?>, currencyConvertedTransactions: List<Map<String, Any>>, historicalNavs: List<HistoricalNavDto>) {
        val withdrawalTransaction = currencyConvertedTransactions.firstOrNull { it["type"] in arrayOf(TransactionType.TRANSFER_OUT, TransactionType.INTERNAL_TRANSFER_OUT) }
        if (withdrawalTransaction != null) {
            portfolioMap["historicalNavs"] = historicalNavs
            portfolioMap["dormantSince"] = withdrawalTransaction["date"]
            portfolioMap["closingValue"] = withdrawalTransaction["amount"]
        }
    }

    abstract fun getPortfolio(): MutableMap<String, Any?>

    /**
     * Inserts the withdrawable amounts of each type (partial, quick) for each active currency into
     * the passed portfolio map.
     *
     * @param portfolioMap Portfolio map to insert the withdrawable amounts to
     */
    protected fun insertWithdrawableAmounts(portfolioMap: MutableMap<String, Any?>, investorId: Long, portfolioDetails: PortfolioDetails) {
        val portfolioId = portfolioMap["id"].toString().toLong()
        val partialWithdrawableAmounts = mutableMapOf<Currency, BigDecimal>()
        val quickWithdrawableAmounts = mutableMapOf<Currency, BigDecimal>()
        val activeCurrencies = geoContext.currencyService.getActiveCurrencies()

        val withdrawableAmountInReportingCurrency = fmsDataTransformationService.getWithdrawableAmountWithoutBonusAmount(portfolioId, investorId, reportingCurrency, portfolioDetails.withdrawableAmounts)

        activeCurrencies.forEach {
            partialWithdrawableAmounts[it] = geoContext.currencyService.convert(reportingCurrency, it, withdrawableAmountInReportingCurrency.allowedPartialWithdrawalAmount, serviceDate)
            quickWithdrawableAmounts[it] = geoContext.currencyService.convert(reportingCurrency, it, withdrawableAmountInReportingCurrency.allowedQuickWithdrawalAmount, serviceDate)
        }

        portfolioMap["withdrawableAmounts"] = partialWithdrawableAmounts
        portfolioMap["quickWithdrawableAmounts"] = quickWithdrawableAmounts
    }
}
