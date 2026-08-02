<?php

declare(strict_types=1);

/*
 * Parse every GitHub Actions workflow and the Dependabot config.
 *
 * GitHub rejects an unparseable workflow with a run that fails in 0 seconds and no job
 * logs, which is slow and confusing to diagnose from the outside. The usual culprit is a
 * plain YAML scalar containing ": " — a `run:` line with a PHP ternary is enough to
 * invalidate the entire file.
 *
 * Usage: php tools/lint-workflows.php [file ...]
 */

$autoload = __DIR__.'/../vendor/autoload.php';
if (! is_file($autoload)) {
    fwrite(STDERR, "Run `composer install` first.\n");
    exit(1);
}
require $autoload;

use Symfony\Component\Yaml\Exception\ParseException;
use Symfony\Component\Yaml\Yaml;

$files = array_slice($argv, 1);
if ($files === []) {
    $root  = dirname(__DIR__);
    $files = array_merge(
        glob($root.'/.github/workflows/*.yml') ?: [],
        glob($root.'/.github/workflows/*.yaml') ?: [],
        array_filter([$root.'/.github/dependabot.yml'], 'is_file'),
    );
}

if ($files === []) {
    fwrite(STDERR, "No workflow files found.\n");
    exit(1);
}

$failed = false;
foreach ($files as $file) {
    $name = basename($file);
    try {
        $data = Yaml::parseFile($file);
    } catch (ParseException $e) {
        $failed = true;
        fwrite(STDERR, "FAIL $name: {$e->getMessage()}\n");

        continue;
    }

    if (! is_array($data)) {
        $failed = true;
        fwrite(STDERR, "FAIL $name: did not parse to a mapping\n");

        continue;
    }

    $detail = isset($data['jobs']) && is_array($data['jobs'])
        ? count($data['jobs']).' job(s): '.implode(', ', array_keys($data['jobs']))
        : 'config';

    echo "OK   $name — $detail\n";
}

exit($failed ? 1 : 0);
