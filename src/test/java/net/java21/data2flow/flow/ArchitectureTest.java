package net.java21.data2flow.flow;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import net.java21.data2flow.contracts.test.arch.Data2flowArchRules;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * design/testing/backend.md §6 공통 규칙(조직 조건, Thread.sleep·시스템 시계 금지)과 flow-engine 규칙: GraalJS는 polyglot API로만,
 * controller → service → repository, 실행 계획·노드 SPI(plan.domain)는 Spring에 의존하지 않음.
 */
@AnalyzeClasses(packages = "net.java21.data2flow.flow", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule organizationScoped = Data2flowArchRules.REPOSITORY_QUERIES_ARE_ORGANIZATION_SCOPED;
    @ArchTest
    static final ArchRule noUnscopedCrud = Data2flowArchRules.UNSCOPED_CRUD_LOOKUPS_ARE_NOT_CALLED;
    @ArchTest
    static final ArchRule noSleep = Data2flowArchRules.NO_THREAD_SLEEP;
    @ArchTest
    static final ArchRule noSystemClock = Data2flowArchRules.NO_SYSTEM_CLOCK;

    @ArchTest
    static final ArchRule polyglotApiOnly = noClasses().should().dependOnClassesThat()
            .resideInAnyPackage("com.oracle.truffle..", "com.oracle.js..");

    @ArchTest
    static final ArchRule repositoriesDoNotUseServices = noClasses().that().resideInAPackage("..repository..")
            .should().dependOnClassesThat().resideInAnyPackage("..service..", "..controller..");

    @ArchTest
    static final ArchRule servicesDoNotUseControllers = noClasses().that().resideInAPackage("..service..")
            .should().dependOnClassesThat().resideInAPackage("..controller..");

    @ArchTest
    static final ArchRule controllersDoNotUseRepositories = noClasses().that().resideInAPackage("..controller..")
            .should().dependOnClassesThat().resideInAPackage("..repository..");

    @ArchTest
    static final ArchRule domainIsPlain = noClasses().that().resideInAPackage("..domain..")
            .should().dependOnClassesThat().resideInAnyPackage("org.springframework..");
}
