from pyeemd import emd_num_imfs, emd, eemd, ceemdan


def eemd_analysis(timeseries, s_num, max_siftings, noise_strength, size_k):
    if noise_strength is None:
        noise_strength = 0.2
    if size_k is None:
        size_k = 250
    imf = eemd(timeseries, S_number=s_num, num_siftings=max_siftings, noise_strength=noise_strength,
                ensemble_size=size_k)
    return imf


def ceemd_analysis(timeseries, s_num, max_siftings, noise_strength, size_k):
    if noise_strength is None:
        noise_strength = 0.2
    if size_k is None:
        size_k = 250
    imf = ceemdan(timeseries, S_number=s_num, num_siftings=max_siftings,
                  noise_strength=noise_strength, ensemble_size=size_k)
    return imf

