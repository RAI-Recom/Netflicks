def hybrid_score(cf_score, cb_score, num_rated):
    if num_rated >= 10:
        return 0.8 * cf_score + 0.2 * cb_score
    elif num_rated > 0:
        return 0.5 * cf_score + 0.5 * cb_score
    else:
        return cb_score
