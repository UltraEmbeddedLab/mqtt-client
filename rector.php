<?php

declare(strict_types=1);

use Rector\Config\RectorConfig;

/*
 * Rector reports a broken configuration by throwing InvalidConfigurationException while it
 * starts — the right place for it to surface, so the inspection is silenced, not handled.
 */
/** @noinspection PhpUnhandledExceptionInspection */
return RectorConfig::configure()
    ->withPaths([
        __DIR__.'/src',
        __DIR__.'/tests',
        __DIR__.'/tools',
        // Not public API, but people copy examples into their own code and the benchmarks
        // are how performance claims get checked, so both stay on the same PHP baseline.
        __DIR__.'/benchmarks',
        __DIR__.'/examples',
    ])
    ->withSkip([
        // The developer's local, gitignored copy of config.php.dist — not ours to rewrite.
        __DIR__.'/examples/config.php',
    ])
    ->withRootFiles()
    ->withCache(__DIR__.'/.rector.cache')
    // Rector writes the types it adds as fully qualified names; this turns them into `use`
    // imports. Global classes (\Exception, \Throwable) are left to Pint, which imports
    // those under its own global_namespace_import rule and orders the result.
    ->withImportNames(importShortClasses: false)
    // No argument: the target is read from `require.php` in composer.json (^8.5), so raising
    // the minimum there is the only edit needed. This is the form Rector itself recommends;
    // naming a version here as well only gave the two a chance to drift apart.
    //
    // The PHP 8.5 set fixes 8.5 deprecations (chr()/ord() ranges, casts, backticks, ...)
    // and adopts array_first()/array_last(). The opt-in pipe-operator rules are left off on
    // purpose: they rewrite nested calls into `|>` chains, a style choice rather than a fix.
    ->withPhpSets()
    ->withPreparedSets(
        deadCode: true,
        // Also carries the early-return rules; the separate earlyReturn set is empty and
        // deprecated in Rector 2.
        codeQuality: true,
        typeDeclarations: true,
        // Precise array shapes and generics in docblocks, where PHPStan max relies on them.
        typeDeclarationDocblocks: true,
        privatization: true,
        // The rules Rector's maintainers run on their own code.
        rectorPreset: true,
        // Not enabled, deliberately:
        // - codingStyle rewrites string interpolation into sprintf() across the codebase,
        //   against the interpolated style it standardised on;
        // - naming, instanceOf and if are deprecated in Rector 2;
        // - phpunit* target PHPUnit test classes — this project's tests are Pest functions.
    );
