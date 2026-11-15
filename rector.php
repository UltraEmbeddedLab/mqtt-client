<?php

declare(strict_types=1);

use Rector\Config\RectorConfig;

return RectorConfig::configure()
    ->withPaths([
        __DIR__.'/src',
        __DIR__.'/tests',
        __DIR__.'/tools',
    ])
    ->withRootFiles()
    ->withCache(__DIR__.'/.rector.cache')
    // withPhpSets()/withPreparedSets() replace the deprecated LevelSetList and SetList
    // constants. Same rule coverage, but the target version is declarative. Tracks the
    // `php` constraint in composer.json — raise both together.
    ->withPhpSets(php85: true)
    // strictBooleans was dropped: Rector deprecates it as "mostly risky and not practical"
    // and points at codeQuality, which already carries granular, stable equivalents.
    // Its other suggestion, codingStyle, is deliberately NOT enabled — it rewrites string
    // interpolation into sprintf() across 29 files, against the interpolated style this
    // codebase standardised on.
    ->withPreparedSets(
        deadCode: true,
        codeQuality: true,
        typeDeclarations: true,
        privatization: true,
        earlyReturn: true,
    );
