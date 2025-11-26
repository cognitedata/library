# Template library

This repository contains Cognite Templates as downloadable and extensible
configuration that can be downloaded and adapted, then deployed with the
[Cognite Toolkit](https://docs.cognite.com/cdf/deploy/cdf_toolkit/).

> The Cognite Template is a reusable blueprint that guides users through
> deploying, customizing, and building Cognite solutions. The template can be
> part of a specific business use case for data processing, contextualization
> pipelines for enriching and managing data, and front-end user screens for
> seamless interaction with the system.

![Cognite Toolkit Template Modules](templates.png)

## Current Release


```toml
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:..."
```

## Usage

### 1. Add library reference to cdf.toml


**Note:** The checksum below is a placeholder. For the current checksum, check the [latest release](https://github.com/cognitedata/library/releases/latest) or click the release badge above.

Add this to your cdf.toml file: 
```
[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:..."
```

### 2. Enable alpha flag

**Note:** If your Toolkit version is 0.7.0 or higher, you can skip this step

Add this to your cdf.toml file: 
```
[alpha_flags] 
external-libraries = true
```

### 3. Run the init command

Run `cdf modules init` (new repo) or `cdf modules add` (existing repo). The Toolkit will present an interactive menu of the Deployment Packs offered. 


## Disclaimer

The open-source Github repository ("Repository") is provided "as is", without
warranty of any kind, express or implied, including but not limited to the
warranties of merchantability, fitness for a particular purpose, and
non-infringement. Usage of the Repository is voluntary and in no event shall
Cognite be liable for any claim, damages, or other liability, whether in an
action of contract, tort, or otherwise, arising from, out of, or in connection
with the use of the Repository.
